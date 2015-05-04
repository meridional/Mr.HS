{-# LANGUAGE OverloadedStrings, DeriveGeneric #-}
module Network.MapReduce.Types where


import Data.Aeson
{-import Control.Applicative-}
{-import Control.Monad-}
import GHC.Generics


-- | Defines the data to pass to the worker 
data WorkerCmd = WorkerCmd {
          wcWorkerID :: Int
        , wcStageID :: Int
        , wcInput :: [String]
        , wcReducerCount :: Int
        } deriving (Show, Generic)

instance FromJSON WorkerCmd
instance ToJSON WorkerCmd

