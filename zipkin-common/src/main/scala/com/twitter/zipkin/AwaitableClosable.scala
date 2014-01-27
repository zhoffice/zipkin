package com.twitter.zipkin

import com.twitter.util._

trait AwaitableClosable extends Closable with CloseAwaitably

object AwaitableClosable {
  def sequence(cs: Closable*): AwaitableClosable = new AwaitableClosable {
    def close(deadline: Time): Future[Unit] =
      closeAwaitably { Closable.sequence(cs: _*).close(deadline) }
  }

  def all(cs: Closable*): AwaitableClosable = new AwaitableClosable {
    def close(deadline: Time): Future[Unit] =
      closeAwaitably { Closable.all(cs: _*).close(deadline) }
  }
}
