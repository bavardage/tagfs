import Database.Redis.Monad.State (runWithRedis)
import Database.Redis.Monad hiding (connect)
import Database.Redis.Redis (connect)
import Control.Monad.Trans
import Control.Monad (liftM)

------------------------------------------------------------
-- Utils
------------------------------------------------------------


------------------------------------------------------------
-- Database stuff
------------------------------------------------------------

--differentiate keys for filenames, keys for tags
--so things don't die if people decide tagging things
--  with sha1 hashes is a good idea
filenameTag, tagTag, tempTag :: String -> String
filenameTag f = '~' : f
tagTag t = '!' : t
tempTag t = '?' : t

addFileTag :: WithRedis m => String -> String -> m ()
addFileTag f t = do
  sadd (filenameTag f) t
  sadd (tagTag t) f
  return ()

removeFileTag :: WithRedis m => String -> String -> m ()
removeFileTag f t = do
  srem (filenameTag f) t
  srem (tagTag t) f
  return ()

getTagsForFile, getFilesForTag :: (WithRedis m) => String -> m (Reply String)
getTagsForFile = smembers . filenameTag
getFilesForTag = smembers . tagTag



data BooleanTree = Union BooleanTree BooleanTree [String]
                 | Intersect BooleanTree BooleanTree [String]
                 | Tag String [String]
                 | TempTag String [String] deriving Show
---oooo dear so many problems, need to kill the temporary tags
foldBT :: WithRedis m => BooleanTree -> m (BooleanTree)
foldBT (Intersect a b _) = do
  (x,xs) <- liftM detag' $ foldBT a
  (y,ys) <- liftM detag' $ foldBT b
  let tt = tempTag $ '^' : x ++ y
  sinterStore tt [tagTag x, tagTag y]
  return $ TempTag tt (xs ++ ys)
foldBT (Union a b _) = do
  (x, xs) <- liftM detag' $ foldBT a
  (y, ys) <- liftM detag' $ foldBT b
  let tt = tempTag $ '|' : x ++ y --random chars to avoid conflicts
  sunionStore tt [tagTag x, tagTag y]
  return $ TempTag tt (xs ++ ys)
foldBT (Tag s _) = return (Tag s [])
foldBT (TempTag s _) = return (TempTag s [])
detag' (Tag x ts) = (x, ts)
detag' (TempTag x ts) = (x, ts)
------------------------------------------------------------
-- Fuse stuff
------------------------------------------------------------


------------------------------------------------------------
-- Testy bleh stuff
------------------------------------------------------------
main = do
  redis <- connect localhost defaultPort
  runWithRedis redis go

go = do
  addFileTag "pic1.jpg" "cat"
  addFileTag "pic1.jpg" "lol"


