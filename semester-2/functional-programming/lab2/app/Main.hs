module Main where
import Data.Time
import Integral

f :: Double -> Double
f x = x^3

main = do
     start <- getCurrentTime
     print (calculate 0 10 0.00005 10 f)
     end <- getCurrentTime
     print (diffUTCTime end start)
