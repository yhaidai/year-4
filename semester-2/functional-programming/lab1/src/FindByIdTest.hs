{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}

module FindByIdTest where
import Data.List
import Data.String
import Database.MySQL.Base
import System.IO
import qualified System.IO.Streams as Streams
import qualified Data.Text as Text
import Data.Typeable
import Data.Text.Conversions
import Control.Monad
import Test.Tasty (testGroup)
import Test.Tasty.HUnit (assertEqual, assertFailure, testCase)
import Create
import Delete
import Update
import Utils
import Control.Concurrent

findAuthorTest = 
    testCase "Find author" $ do
        conn <- connect defaultConnectInfo {ciUser = "root", ciPassword = "123321", ciDatabase = "resources_test"}

        createRow Authors ["NewAuthorName", "NewAuthorSurname"] conn
        threadDelay 1000000

        (defs, is) <- query_ conn "SELECT name FROM authors where id = 100"
        list <- Streams.toList is
        forM_ list $ \[MySQLText name, MySQLText surname] -> do
            assertEqual "Find test name" "NewAuthorName" name
            assertEqual "Find test surname" "NewAuthorSurname" surname

        deleteRow Authors "1" conn
        threadDelay 2000000

        close conn