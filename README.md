# Mr.HS - a slightly generalized MapReduce framework in Haskell

## To build
First you need: 
1. Haskell compiler [GHC]("https://www.haskell.org/ghc/")
2. build tool [cabal]("https://www.haskell.org/cabal/").
The easy way to install these two is to install
[the Haskell Platform]("https://www.haskell.org/platform/").

Second, in the root dir of this repo, type
```.shell
cabal install
```

## To use
The code for master is in `Network.MapReduce.Master` module.
The code for a generalized multi-stage map reduce worker is in `Network.MapReduce.Worker` module.
You can also find an implementation of the classic type safe MapReduce on top
of Mr. HS in `Network.MapReduce.Classic` module.

Master and Worker communicate via WebSockets by exchaning messages in JSON format.
So Master program is can interface with worker servers implemented in any 
programming languages with any framework. Of course, the implementation that 
I provided here is in Haskell.
