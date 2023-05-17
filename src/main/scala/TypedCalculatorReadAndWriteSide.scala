import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorSystem, Behavior, Props}
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.{EventEnvelope, PersistenceQuery}
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}
import akka.stream.alpakka.slick.scaladsl.{Slick, SlickSession}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, RunnableGraph, Sink, Source}
import akka.stream.{ClosedShape, FlowShape, SinkShape, SourceShape}
import akka.{Done, NotUsed}
import akka_typed.CalculatorRepository.{getLatestsOffsetAndResult, updatedResultAndOffset}
import slick.jdbc.GetResult

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}

object  akka_typed{

  trait CborSerialization

  val persId = PersistenceId.ofUniqueId("001")

  case class Result(id: Long, state: Double, offset: Long)

  sealed trait Command
  case class Add(amount: Int) extends Command
  case class Multiply(amount: Int) extends Command
  case class Divide(amount: Int) extends Command


  sealed trait Event
  case class Added(id: Int, amount: Int) extends Event
  case class Multiplied(id: Int, amount: Int) extends Event
  case class Divided(id: Int, amount: Int) extends Event

  final case class State(value: Int) extends CborSerialization {
    def add(amount: Int): State = copy(value = value + amount)
    def multiply(amount: Int): State = copy(value = value * amount)
    def divide(amount: Int): State = copy(value = value / amount)
  }

  object State {
    val empty = State(0)
  }

  object TypedCalculatorWriteSide{

    def handleCommand(
                     persistenceId: String,
                     state: State,
                     command: Command,
                     ctx: ActorContext[Command]
                     ): Effect[Event, State] =
      command match {
        case Add(amount) =>
          ctx.log.info(s"receive adding  for number: $amount and state is ${state.value}")
          val added = Added(persistenceId.toInt, amount)
          Effect
          .persist(added)
          .thenRun{
            x=> ctx.log.info(s"The state result is ${x.value}")
          }
        case Multiply(amount) =>
          ctx.log.info(s"receive multiplying  for number: $amount and state is ${state.value}")
          val multiplied = Multiplied(persistenceId.toInt, amount)
          Effect
            .persist(multiplied)
            .thenRun{
              x=> ctx.log.info(s"The state result is ${x.value}")
            }
        case Divide(amount) =>
          ctx.log.info(s"receive dividing  for number: $amount and state is ${state.value}")
          val divided = Divided(persistenceId.toInt, amount)
          Effect
            .persist(divided)
            .thenRun{
              x=> ctx.log.info(s"The state result is ${x.value}")
            }
      }

    def handleEvent(state: State, event: Event, ctx: ActorContext[Command]): State =
      event match {
        case Added(_, amount) =>
          ctx.log.info(s"Handling event Added is: $amount and state is ${state.value}")
          state.add(amount)
        case Multiplied(_, amount) =>
          ctx.log.info(s"Handling event Multiplied is: $amount and state is ${state.value}")
          state.multiply(amount)
        case Divided(_, amount) =>
          ctx.log.info(s"Handling event Divided is: $amount and state is ${state.value}")
          state.divide(amount)
      }

    def apply(): Behavior[Command] =
      Behaviors.setup{ ctx =>
        EventSourcedBehavior[Command, Event, State](
          persistenceId = persId,
          State.empty,
          (state, command) => handleCommand("001", state, command, ctx),
          (state, event) => handleEvent(state, event, ctx)
        )
      }
  }

  case class TypedCalculatorReadSide(system: ActorSystem[NotUsed])(implicit slickSession: SlickSession) {
    implicit val materializer = system.classicSystem

    var (id,  state, offset) = Result.unapply(getLatestsOffsetAndResult).get

    val startOffset: Long = if (offset == 1) 1 else offset + 1

    val readJournal = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)


    val source: Source[EventEnvelope, NotUsed] = readJournal.eventsByPersistenceId("001", startOffset, Long.MaxValue)

    private def updateState(event: Any, seqNum: Long): Result = {
      val newState: Double = event match {
        case Added(_, amount) =>
          println(s"Log from Added: $state")
          state + amount
        case Multiplied(_, amount) =>
          println(s"Log from Multiplied: $state")
          state * amount
        case Divided(_, amount) =>
          println(s"Log from Divided: $state")
          state / amount

      }
      Result(id, newState, seqNum)
    }

    private val graph = GraphDSL.create() {

      implicit builder: GraphDSL.Builder[NotUsed] =>
        import GraphDSL.Implicits._

        val input: SourceShape[EventEnvelope] = builder.add(source)
        val stateUpdater: FlowShape[EventEnvelope, Result] = builder.add(Flow[EventEnvelope].map(e => updateState(e.event, e.sequenceNr)))
        val localSaveOutput: SinkShape[Result] = builder.add(Sink.foreach[Result] {
          r =>
            println(s"Local save of $r")
            state = r.state
        })

        val dbSaveOutput: SinkShape[Result] = builder.add(updatedResultAndOffset)

        val broadcast = builder.add(Broadcast[Result](outputPorts = 2))

        input ~> stateUpdater ~> broadcast

        broadcast.out(0) ~> localSaveOutput
        broadcast.out(1) ~> dbSaveOutput

        ClosedShape
    }

    RunnableGraph.fromGraph(graph).run()

  }

  object CalculatorRepository{

    implicit val getResult: GetResult[Result] = GetResult(r => Result(r.nextLong(), r.nextDouble(), r.nextLong()))

    def getLatestsOffsetAndResult(implicit slickSession: SlickSession): Result = {
      import slickSession.profile.api._
      Await.result(
        slickSession.db.run(sql"select * from public.result where id = 1;".as[Result].headOption),
        atMost = 3.seconds
      ).get
    }


    def updatedResultAndOffset(implicit slickSession: SlickSession): Sink[Result, Future[Done]] = {
      import slickSession.profile.api._

      Slick.sink[Result]{
        it: Result =>
          sqlu"update public.result set calculated_value = ${it.state}, write_side_offset = ${it.offset} where id = ${it.id}"
      }
    }
  }

  def apply(): Behavior[NotUsed] =
    Behaviors.setup{
      ctx =>
        val writeAcorRef = ctx.spawn(TypedCalculatorWriteSide(), "Calc", Props.empty)
        writeAcorRef ! Add(10)
        writeAcorRef ! Multiply(2)
        writeAcorRef ! Divide(5)

        Behaviors.same
    }

  def execute(command: Command): Behavior[NotUsed] =
    Behaviors.setup{ ctx =>
      val writeAcorRef = ctx.spawn(TypedCalculatorWriteSide(), "Calc", Props.empty)
      writeAcorRef ! command
      Behaviors.same
    }

  def main(args: Array[String]): Unit = {
    val value = akka_typed()
    implicit  val system: ActorSystem[NotUsed] = ActorSystem(value, "akka_typed")

    implicit val slickSession: SlickSession = SlickSession.forConfig("slick-postgres")

    TypedCalculatorReadSide(system)
    implicit val executionContext = system.executionContext

    system.whenTerminated.onComplete(_ => slickSession.close())
  }

}