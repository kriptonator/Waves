package com.wavesplatform.matcher

import akka.actor.{ActorRef, ActorSystem, PoisonPill, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import cats.kernel.Monoid
import com.wavesplatform.account.{Address, PrivateKeyAccount, PublicKeyAccount}
import com.wavesplatform.matcher.AddressActor.{BalanceUpdated, PlaceOrder}
import com.wavesplatform.matcher.api.AlreadyProcessed
import com.wavesplatform.matcher.model.LimitOrder
import com.wavesplatform.matcher.queue.QueueEvent
import com.wavesplatform.state.{ByteStr, LeaseBalance, Portfolio}
import com.wavesplatform.transaction.assets.exchange.{AssetPair, Order, OrderType, OrderV1}
import com.wavesplatform.wallet.Wallet
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class AddressActorSpecification
    extends TestKit(ActorSystem("AddressActorSpecification"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ImplicitSender {

  private val assetId    = ByteStr("asset".getBytes)
  private val matcherFee = 30000L

  private val sellTokenOrder1 = OrderV1(
    sender = privateKey("test"),
    matcher = PublicKeyAccount("matcher".getBytes()),
    pair = AssetPair(None, Some(assetId)),
    orderType = OrderType.BUY,
    price = 100000000L,
    amount = 100L,
    timestamp = 1L,
    expiration = 1000L,
    matcherFee = matcherFee
  )

  private val sellTokenOrder2 = OrderV1(
    sender = privateKey("test"),
    matcher = PublicKeyAccount("matcher".getBytes()),
    pair = AssetPair(None, Some(assetId)),
    orderType = OrderType.BUY,
    price = 100000000L,
    amount = 100L,
    timestamp = 2L,
    expiration = 1000L,
    matcherFee = matcherFee
  )

  private val sellWavesOrder = OrderV1(
    sender = privateKey("test"),
    matcher = PublicKeyAccount("matcher".getBytes()),
    pair = AssetPair(None, Some(assetId)),
    orderType = OrderType.SELL,
    price = 100000000L,
    amount = 100L,
    timestamp = 3L,
    expiration = 1000L,
    matcherFee = matcherFee
  )

  "AddressActorSpecification" should {
    "cancel orders" when {
      "asset balance changed" in test { (ref, eventsProbe) =>
        val initPortfolio = requiredPortfolio(sellTokenOrder1)
        ref ! BalanceUpdated(initPortfolio)

        ref ! PlaceOrder(sellTokenOrder1)
        eventsProbe.expectMsg(QueueEvent.Placed(sellTokenOrder1))

        ref ! BalanceUpdated(initPortfolio.copy(assets = Map.empty))
        eventsProbe.expectMsg(QueueEvent.Canceled(sellTokenOrder1.assetPair, sellTokenOrder1.id()))
      }

      "waves balance changed" when {
        "there are waves for fee" in wavesBalanceTest(restWaves = matcherFee)
        "there are no waves at all" in wavesBalanceTest(restWaves = 0L)

        def wavesBalanceTest(restWaves: Long): Unit = test { (ref, eventsProbe) =>
          val initPortfolio = requiredPortfolio(sellWavesOrder)
          ref ! BalanceUpdated(initPortfolio)

          ref ! PlaceOrder(sellWavesOrder)
          eventsProbe.expectMsg(QueueEvent.Placed(sellWavesOrder))

          ref ! BalanceUpdated(initPortfolio.copy(balance = restWaves))
          eventsProbe.expectMsg(QueueEvent.Canceled(sellWavesOrder.assetPair, sellWavesOrder.id()))
        }
      }

      "waves were leased" when {
        "there are waves for fee" in leaseTest(_ => matcherFee)
        "there are no waves at all" in leaseTest(_.spendableBalance)

        def leaseTest(leasedWaves: Portfolio => Long): Unit = test { (ref, eventsProbe) =>
          val initPortfolio = requiredPortfolio(sellWavesOrder)
          ref ! BalanceUpdated(initPortfolio)

          ref ! PlaceOrder(sellWavesOrder)
          eventsProbe.expectMsg(QueueEvent.Placed(sellWavesOrder))

          ref ! BalanceUpdated(initPortfolio.copy(lease = LeaseBalance(0, leasedWaves(initPortfolio))))
          eventsProbe.expectMsg(QueueEvent.Canceled(sellWavesOrder.assetPair, sellWavesOrder.id()))
        }
      }
    }

    "track canceled orders and don't cancel more on same BalanceUpdated message" in test { (ref, eventsProbe) =>
      val sellToken1Portfolio = requiredPortfolio(sellTokenOrder1)
      val initPortfolio       = Monoid.combine(sellToken1Portfolio, requiredPortfolio(sellTokenOrder2))
      ref ! BalanceUpdated(initPortfolio)

      ref ! PlaceOrder(sellTokenOrder1)
      eventsProbe.expectMsg(QueueEvent.Placed(sellTokenOrder1))

      ref ! PlaceOrder(sellTokenOrder2)
      eventsProbe.expectMsg(QueueEvent.Placed(sellTokenOrder2))

      ref ! BalanceUpdated(sellToken1Portfolio)
      eventsProbe.expectMsg(QueueEvent.Canceled(sellTokenOrder2.assetPair, sellTokenOrder2.id()))

      ref ! BalanceUpdated(sellToken1Portfolio)
      eventsProbe.expectNoMessage()
    }

    "cancel multiple orders" in test { (ref, eventsProbe) =>
      val sellWavesPortfolio = requiredPortfolio(sellWavesOrder)
      val initPortfolio      = Monoid.combineAll(Seq(requiredPortfolio(sellTokenOrder1), requiredPortfolio(sellTokenOrder2), sellWavesPortfolio))
      ref ! BalanceUpdated(initPortfolio)

      ref ! PlaceOrder(sellTokenOrder1)
      eventsProbe.expectMsg(QueueEvent.Placed(sellTokenOrder1))

      ref ! PlaceOrder(sellTokenOrder2)
      eventsProbe.expectMsg(QueueEvent.Placed(sellTokenOrder2))

      ref ! BalanceUpdated(sellWavesPortfolio)
      eventsProbe.expectMsg(QueueEvent.Canceled(sellTokenOrder1.assetPair, sellTokenOrder1.id()))
      eventsProbe.expectMsg(QueueEvent.Canceled(sellTokenOrder2.assetPair, sellTokenOrder2.id()))
    }

    "cancel only orders, those aren't fit" in test { (ref, eventsProbe) =>
      val sellWavesPortfolio = requiredPortfolio(sellWavesOrder)
      val initPortfolio      = Monoid.combineAll(Seq(requiredPortfolio(sellTokenOrder1), requiredPortfolio(sellTokenOrder2), sellWavesPortfolio))
      ref ! BalanceUpdated(initPortfolio)

      ref ! PlaceOrder(sellTokenOrder1)
      eventsProbe.expectMsg(QueueEvent.Placed(sellTokenOrder1))

      ref ! PlaceOrder(sellWavesOrder)
      eventsProbe.expectMsg(QueueEvent.Placed(sellWavesOrder))

      ref ! PlaceOrder(sellTokenOrder2)
      eventsProbe.expectMsg(QueueEvent.Placed(sellTokenOrder2))

      ref ! BalanceUpdated(sellWavesPortfolio)
      eventsProbe.expectMsg(QueueEvent.Canceled(sellTokenOrder1.assetPair, sellTokenOrder1.id()))
      eventsProbe.expectMsg(QueueEvent.Canceled(sellTokenOrder2.assetPair, sellTokenOrder2.id()))
    }
  }

  private def test(f: (ActorRef, TestProbe) => Unit): Unit = {
    val eventsProbe = TestProbe()
    val addressActor = system.actorOf(
      Props(
        new AddressActor(
          addr("test"),
          1.day,
          1.day,
          EmptyOrderDB,
          event => {
            eventsProbe.ref ! event
            Future.successful(AlreadyProcessed)
          }
        )))
    f(addressActor, eventsProbe)
    addressActor ! PoisonPill
  }

  private def requiredPortfolio(order: Order): Portfolio = {
    val b = LimitOrder(order).requiredBalance
    Portfolio(b.getOrElse(None, 0L), LeaseBalance.empty, b.collect { case (Some(id), v) => id -> v })
  }

  private def addr(seed: String): Address                 = privateKey(seed).toAddress
  private def privateKey(seed: String): PrivateKeyAccount = Wallet.generateNewAccount(seed.getBytes(), 0)

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }
}
