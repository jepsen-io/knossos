# Knossos

<img src="https://raw.github.com/aphyr/knossos/master/doc/linear-b.jpg" alt="Linear B" />

Given a history of operations by a set of clients, and some singlethreaded
model, attempts to show that the history is not linearizable with respect to
that model. I am not certain the algorithm is correct yet; you should treat its
results as plausible but verify by hand.

Named after the ruins where the Linear B script was discovered. You know,
because we're testing if the history "be linear". YES IT'S A HISTORY PUN WORK
WITH ME HERE OKAY?

See `knossos.core` for the linearizability checker, and `knossos.redis` for an
model that can generate histories verifiable with Knossos. `knossos.redis-test`
synthesizes the two, testing the redis model for linearizability failures.

Read the [blog
post](http://aphyr.com/posts/309-knossos-redis-and-linearizability) for a full apology for releasing this monstrosity.

## Installation

Via [Clojars](https://clojars.org/knossos), as usual.

## License

Copyright © 2013 Kyle Kingsbury

Distributed under the Eclipse Public License, the same as Clojure.
