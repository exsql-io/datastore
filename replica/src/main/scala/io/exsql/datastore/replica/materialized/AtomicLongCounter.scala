package io.exsql.datastore.replica.materialized

import java.util.concurrent.atomic.AtomicLong

class AtomicLongCounter(private val initial: Long) {

  private val counter = new AtomicLong(initial)

  def increment(amount: Long = 1): Unit = {
    var current = counter.get()
    var incremented = current + amount
    while (!counter.compareAndSet(current, incremented)) {
      current = counter.get()
      incremented = current + amount
    }
  }

  def incrementAndGet(): Long = {
    var current = counter.get()
    var incremented = current + 1
    while (!counter.compareAndSet(current, incremented)) {
      current = counter.get()
      incremented = current + 1
    }

    incremented
  }

  def decrement(amount: Long = 1): Unit = {
    var current = counter.get()
    var decremented = current - amount
    while (!counter.compareAndSet(current, decremented)) {
      current = counter.get()
      decremented = current - amount
    }
  }

  def get(): Long = counter.get()

}

object AtomicLongCounter {
  def apply(initial: Long = 0L): AtomicLongCounter = new AtomicLongCounter(initial)
}