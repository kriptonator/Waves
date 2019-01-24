package com.wavesplatform.matcher.model

import com.wavesplatform.matcher.model.Events._
import com.wavesplatform.matcher.model.MatcherModel.{Level, Price}
import com.wavesplatform.state.ByteStr
import com.wavesplatform.transaction.assets.exchange.{Order, OrderType}

import scala.annotation.tailrec
import scala.collection.immutable
import scala.collection.immutable.TreeMap

class OrderBook(private var _bids: OrderBook.Side, private var _asks: OrderBook.Side) {
  import OrderBook._

  /** @param canMatch (submittedPrice, counterPrice) => Boolean */
  @tailrec private def doMatch(eventTs: Long,
                               canMatch: (Long, Long) => Boolean,
                               submitted: LimitOrder,
                               prevEvents: Seq[Event],
                               side: Side): (Seq[Event], Side) =
    side.best match {
      case None                                                           => (OrderAdded(submitted) +: prevEvents, side)
      case Some(bestOrder) if !canMatch(submitted.price, bestOrder.price) => (OrderAdded(submitted) +: prevEvents, side)
      case Some(bestOrder) =>
        if (!bestOrder.order.isValid(eventTs)) {
          doMatch(eventTs, canMatch, submitted, OrderCanceled(bestOrder, true) +: prevEvents, side.removeBest())
        } else {
          val x = OrderExecuted(submitted, bestOrder)
          require(
            x.counterRemaining.isValid != x.submittedRemaining.isValid,
            s"Either submitted ${submitted.order.id()} or counter ${bestOrder.order.id()} must match completely"
          )

          if (x.submittedRemaining.isValid) {
            doMatch(eventTs, canMatch, x.submittedRemaining, x +: prevEvents, side.removeBest())
          } else {
            (x +: prevEvents, side.replaceBest(x.counterRemaining))
          }
        }
    }

  def bids: Iterable[(Price, Level[LimitOrder])]  = _bids
  def bestBid: Option[(Price, Level[LimitOrder])] = _bids.headOption
  def bidOrders: immutable.Iterable[LimitOrder]   = _bids.flatMap(_._2)

  def asks: Iterable[(Price, Level[LimitOrder])]  = _asks
  def bestAsk: Option[(Price, Level[LimitOrder])] = _asks.headOption
  def askOrders: immutable.Iterable[LimitOrder]   = _asks.flatMap(_._2)

  def allOrders: Iterable[LimitOrder] =
    for {
      (_, level) <- _bids ++ _asks
      lo         <- level
    } yield lo

  def cancel(orderId: ByteStr): Option[OrderCanceled] = ???

  def cancelAll(): Seq[OrderCanceled] = {
    val canceledOrders = allOrders.map(lo => OrderCanceled(lo, unmatchable = false)).toSeq

    _bids = TreeMap.empty(bidsOrdering)
    _asks = TreeMap.empty(asksOrdering)
    canceledOrders
  }

  def add(o: Order, ts: Long): Seq[Event] = {
    o.orderType match {
      case OrderType.BUY =>
        val (events, newAsks) = doMatch(ts, _ >= _, LimitOrder(o), Seq.empty, _asks)
        _asks = newAsks
        events
      case OrderType.SELL =>
        val (events, newBids) = doMatch(ts, _ <= _, LimitOrder(o), Seq.empty, _bids)
        _bids = newBids
        events
    }
  }
}

object OrderBook {
  type Side = TreeMap[Price, Level[LimitOrder]]

  implicit class SideExt(val side: Side) extends AnyVal {
    def best: Option[LimitOrder] = side.headOption.flatMap(_._2.headOption)
    def removeBest(): Side = side.headOption.fold(side) {
      case (price, level) =>
        if (level.isEmpty) side.drop(1).removeBest()
        else side + (price -> level.tail)
    }

    def replaceBest(newBestAsk: LimitOrder): Side = {
      require(side.nonEmpty, "Cannot replace best level of an empty side")
      val (price, level) = side.head
      if (level.isEmpty) side.drop(1).replaceBest(newBestAsk)
      else side + (price -> (newBestAsk +: level.tail))
    }
  }

  val bidsOrdering: Ordering[Long] = (x: Long, y: Long) => -Ordering.Long.compare(x, y)
  val asksOrdering: Ordering[Long] = (x: Long, y: Long) => Ordering.Long.compare(x, y)

  def empty: OrderBook = OrderBook(TreeMap.empty(bidsOrdering), TreeMap.empty(asksOrdering))

  def apply(bids: Map[Price, Level[BuyLimitOrder]], asks: Map[Price, Level[SellLimitOrder]]): OrderBook =
    new OrderBook(TreeMap.empty[Price, Level[LimitOrder]](bidsOrdering), TreeMap.empty[Price, Level[LimitOrder]](asksOrdering))
}
