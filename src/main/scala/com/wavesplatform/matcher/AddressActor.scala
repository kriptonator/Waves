package com.wavesplatform.matcher

import akka.actor.Actor
import akka.pattern.pipe
import com.wavesplatform.account.Address
import com.wavesplatform.matcher.Matcher.StoreEvent
import com.wavesplatform.matcher.OrderDB.orderInfoOrdering
import com.wavesplatform.matcher.api._
import com.wavesplatform.matcher.model.Events.{OrderAdded, OrderCanceled, OrderExecuted}
import com.wavesplatform.matcher.model.LimitOrder.OrderStatus
import com.wavesplatform.matcher.model.{LimitOrder, OrderInfo, OrderValidator}
import com.wavesplatform.matcher.queue.QueueEvent
import com.wavesplatform.state.{ByteStr, Portfolio}
import com.wavesplatform.transaction.AssetId
import com.wavesplatform.transaction.assets.exchange.{AssetPair, Order}
import com.wavesplatform.utils.{LoggerFacade, ScorexLogging}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

class AddressActor(
    owner: Address,
    maxTimestampDrift: FiniteDuration,
    cancelTimeout: FiniteDuration,
    orderDB: OrderDB,
    storeEvent: StoreEvent,
) extends Actor
    with ScorexLogging {

  import AddressActor._
  import context.dispatcher

  private type SpendableBalance = Map[Option[AssetId], Long]

  protected override def log = LoggerFacade(LoggerFactory.getLogger(s"AddressActor[$owner]"))

  private var spendableBalance = toSpendable(Portfolio.empty)
  private val activeOrders     = mutable.AnyRefMap.empty[Order.Id, LimitOrder]
  private val openVolume       = mutable.AnyRefMap.empty[Option[AssetId], Long].withDefaultValue(0L)
  private var latestOrderTs    = 0L
  private val markedToCancel   = mutable.Set.empty[Order.Id]

  private def reserve(limitOrder: LimitOrder): Unit =
    for ((id, b) <- limitOrder.requiredBalance if b != 0) {
      val prevBalance = openVolume(id)
      val newBalance  = prevBalance + b
      log.trace(s"${limitOrder.order.id()}: $id -> +$b ($prevBalance -> $newBalance)")
      openVolume += id -> newBalance
    }

  private def release(orderId: ByteStr): Unit =
    for (limitOrder <- activeOrders.get(orderId); (id, b) <- limitOrder.requiredBalance if b != 0) {
      val prevBalance = openVolume(id)
      val newBalance  = prevBalance - b
      log.trace(s"${limitOrder.order.id()}: $id -> -$b ($prevBalance -> $newBalance)")
      openVolume += id -> newBalance
    }

  private def updateOpenVolume(limitOrder: LimitOrder): Unit = {
    release(limitOrder.order.id())
    reserve(limitOrder)
  }

  private def updateTimestamp(newTimestamp: Long): Unit = if (newTimestamp > latestOrderTs) {
    latestOrderTs = newTimestamp
  }

  private def tradableBalance(assetId: Option[AssetId]): Long = spendableBalance(assetId) - openVolume(assetId)

  private val validator =
    OrderValidator.accountStateAware(owner,
                                     tradableBalance,
                                     activeOrders.size,
                                     latestOrderTs - maxTimestampDrift.toMillis,
                                     id => activeOrders.contains(id) || orderDB.contains(id)) _

  private def handleCommands: Receive = {
    case BalanceUpdated(newPortfolio) =>
      // Also if not changed
      log.trace(s"Balance changed: $spendableBalance -> $newPortfolio")
      spendableBalance = toSpendable(newPortfolio)
      val toCancel = ordersToDelete(spendableBalance, activeOrders.values.toVector)
      log.debug(s"Canceling: $toCancel")
      toCancel.foreach(storeEvent)

    case PlaceOrder(o) =>
      log.debug(s"New order: ${o.json()}")
      validator(o) match {
        case Right(_) =>
          updateTimestamp(o.timestamp)
          orderDB.saveOrder(o)
          val lo = LimitOrder(o)
          activeOrders += o.id() -> lo
          reserve(lo)
          latestOrderTs = latestOrderTs.max(lo.order.timestamp)
          storeEvent(QueueEvent.Placed(o)).pipeTo(sender())
        case Left(error) =>
          sender() ! OrderRejected(error)
      }
    case CancelOrder(id) =>
      activeOrders.get(id) match {
        case Some(lo) => storeEvent(QueueEvent.Canceled(lo.order.assetPair, id)).pipeTo(sender())
        case None     => sender() ! OrderCancelRejected(s"Order $id not found")
      }
    case CancelAllOrders(maybePair, timestamp) =>
      if ((timestamp - latestOrderTs).abs <= maxTimestampDrift.toMillis) {
        val batchCancelFutures = for {
          lo <- activeOrders.values
          if maybePair.forall(_ == lo.order.assetPair)
        } yield storeEvent(QueueEvent.Canceled(lo.order.assetPair, lo.order.id())).map(x => lo.order.id() -> x.asInstanceOf[WrappedMatcherResponse])

        Future.sequence(batchCancelFutures).map(_.toMap).map(BatchCancelCompleted).pipeTo(sender())
      } else {
        sender() ! OrderCancelRejected("Invalid timestamp")
      }
  }

  private def handleStatusRequests: Receive = {
    case GetOrderStatus(orderId) =>
      sender() ! activeOrders.get(orderId).fold(orderDB.status(orderId))(activeStatus)
    case GetOrders(maybePair, onlyActive) =>
      log.trace(s"Loading ${if (onlyActive) "active" else "all"} ${maybePair.fold("")(_.toString + " ")}orders")
      val matchingActiveOrders = (for {
        lo <- activeOrders.values
        if maybePair.forall(_ == lo.order.assetPair)
      } yield
        lo.order
          .id() -> OrderInfo(lo.order.orderType, lo.order.amount, lo.order.price, lo.order.timestamp, activeStatus(lo), lo.order.assetPair)).toSeq.sorted

      log.trace(s"Collected ${matchingActiveOrders.length} active orders")

      sender() ! (if (onlyActive) matchingActiveOrders else orderDB.loadRemainingOrders(owner, maybePair, matchingActiveOrders))
    case GetTradableBalance(pair) =>
      sender() ! Set(pair.amountAsset, pair.priceAsset).map(id => id -> tradableBalance(id)).toMap
    case GetReservedBalance =>
      sender() ! openVolume.filter(_._2 > 0).toMap
  }

  private def handleExecutionEvents: Receive = {
    case OrderAdded(submitted) if submitted.order.sender.toAddress == owner =>
      log.trace(s"OrderAdded(${submitted.order.id()})")
      updateTimestamp(submitted.order.timestamp)
      updateOpenVolume(submitted)
      activeOrders += submitted.order.id() -> submitted
    case e @ OrderExecuted(submitted, counter) =>
      log.trace(s"OrderExecuted(${submitted.order.id()}, ${counter.order.id()}), amount = ${e.executedAmount}")
      handleOrderExecuted(e.submittedRemaining)
      handleOrderExecuted(e.counterRemaining)

    case OrderCanceled(lo, unmatchable) if activeOrders.contains(lo.order.id()) =>
      log.trace(s"OrderCanceled(${lo.order.id()}, system=$unmatchable)")
      release(lo.order.id())
      val l = lo.order.amount - lo.amount
      handleOrderTerminated(lo, if (unmatchable) LimitOrder.Filled(l) else LimitOrder.Cancelled(l))
      markedToCancel -= lo.order.id()
  }

  private def handleOrderExecuted(remaining: LimitOrder): Unit = if (remaining.order.sender.toAddress == owner) {
    updateTimestamp(remaining.order.timestamp)
    release(remaining.order.id())
    if (remaining.isValid) {
      reserve(remaining)
      activeOrders += remaining.order.id() -> remaining
    } else {
      val actualFilledAmount = remaining.order.amount - remaining.amount
      handleOrderTerminated(remaining, LimitOrder.Filled(actualFilledAmount))
    }
  }

  private def handleOrderTerminated(lo: LimitOrder, status: OrderStatus): Unit = {
    log.trace(s"Order ${lo.order.id()} terminated: $status")
    activeOrders.remove(lo.order.id())
    orderDB.saveOrderInfo(
      lo.order.id(),
      owner,
      OrderInfo(lo.order.orderType, lo.order.amount, lo.order.price, lo.order.timestamp, status, lo.order.assetPair)
    )
  }

  def receive: Receive = handleCommands orElse handleExecutionEvents orElse handleStatusRequests

  private def ordersToDelete(init: SpendableBalance, orders: IndexedSeq[LimitOrder]): Vector[QueueEvent.Canceled] = {
    // Probably, we need to check orders with changed assets only.
    // Now a user can have 100 active transaction maximum - easy to traverse.
    val ordersByPriority = orders.sortBy(_.order.timestamp)(Ordering[Long]) // Will cancel new orders first
    val (_, r) = ordersByPriority.foldLeft((init, Vector.empty[QueueEvent.Canceled])) {
      case ((rest, toDelete), lo) =>
        val id = lo.order.id()
        if (markedToCancel.contains(id)) (rest, toDelete)
        else {
          val updatedPortfolio = lo.requiredBalance.foldLeft[Option[SpendableBalance]](Some(rest)) {
            case (p, (assetId, requiredValue)) => p.flatMap(remove(_, assetId, requiredValue))
          }

          updatedPortfolio match {
            case None =>
              markedToCancel += id
              (rest, toDelete :+ QueueEvent.Canceled(lo.order.assetPair, id))

            case Some(x) => (x, toDelete)
          }
        }
    }
    r
  }

  private def toSpendable(p: Portfolio): SpendableBalance =
    p.assets
      .map { case (k, v) => (Some(k): Option[AssetId]) -> v }
      .updated(None, p.spendableBalance)
      .withDefaultValue(0)

  private def remove(spendable: SpendableBalance, assetId: Option[AssetId], amount: Long): Option[SpendableBalance] = {
    val updatedAmount = spendable.getOrElse(assetId, 0L) - amount
    if (updatedAmount < 0) None
    else Some(spendable.updated(assetId, updatedAmount))
  }
}

object AddressActor {
  private def activeStatus(lo: LimitOrder): OrderStatus =
    if (lo.amount == lo.order.amount) LimitOrder.Accepted else LimitOrder.PartiallyFilled(lo.order.amount - lo.amount)

  sealed trait Command

  case class GetOrderStatus(orderId: ByteStr)                             extends Command
  case class GetOrders(assetPair: Option[AssetPair], onlyActive: Boolean) extends Command
  case class GetTradableBalance(assetPair: AssetPair)                     extends Command
  case object GetReservedBalance                                          extends Command
  case class PlaceOrder(order: Order)                                     extends Command
  case class CancelOrder(orderId: ByteStr)                                extends Command
  case class CancelAllOrders(pair: Option[AssetPair], timestamp: Long)    extends Command
  case class BalanceUpdated(newPortfolio: Portfolio)                      extends Command

  private case class CancelTimedOut(orderId: ByteStr)
}
