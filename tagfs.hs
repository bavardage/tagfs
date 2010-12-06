import Database.Redis.Monad.State (runWithRedis)
import Database.Redis.Monad hiding (connect)
import Database.Redis.Redis (connect)
import Control.Monad.Trans
import Control.Monad (liftM)
import System.Fuse
import System.Environment (withArgs)
import System.Posix.Files (ownerReadMode, ownerExecuteMode, ownerWriteMode, ownerModes, getFileStatus)

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
data FH = String
directory, regularFile, emptyStat :: FileStat
directory = emptyStat
    { statEntryType = Directory
    , statFileSize = 4096
    , statBlocks = 1
    , statFileMode = foldr1 unionFileModes [ ownerReadMode, ownerExecuteMode ]
    }

regularFile = emptyStat
    { statEntryType = RegularFile
    , statFileMode = foldr1 unionFileModes [ ownerReadMode, ownerWriteMode ]
    }

emptyStat = FileStat
    { statEntryType = Unknown
    , statFileMode = ownerModes
    , statLinkCount = 0
    , statFileOwner = 0
    , statFileGroup = 0
    , statSpecialDeviceID = 0
    , statFileSize = 0
    , statBlocks = 0
    , statAccessTime = 0
    , statModificationTime = 0
    , statStatusChangeTime = 0
    }
-- in future use POSIX getFileStatus

openDirectory :: FilePath -> IO Errno
openDirectory f = return eOK

readDirectory :: FilePath -> IO (Either Errno [(FilePath, FileStat)])
readDirectory f = return $ Right dots
    where dots = [(".", directory), ("..", directory)]

getFileStat :: FilePath -> IO (Either Errno FileStat)
getFileStat f = return $ Right regularFile

--blatantly copied from FunionFS
-- what the hell is the argument even?
getFileSystemStats :: String -> IO (Either Errno FileSystemStats)
getFileSystemStats str =
  return $ Right FileSystemStats
    { fsStatBlockSize  = 512
    , fsStatBlockCount = 1
    , fsStatBlocksFree = 1
    , fsStatBlocksAvailable = 1
    , fsStatFileCount  = 5      -- IS THIS CORRECT? PROBABLY NOT
    , fsStatFilesFree  = 10     -- WHAT IS THIS? HELL IF I KNOW
    , fsStatMaxNameLength = 255 -- SEEMS SMALL? YEAH IT DOES
    }


runFuse :: IO ()
runFuse = do
  withArgs ["/tmp/tagfs2/"] $ fuseMain fuseOps defaultExceptionHandler

fuseOps :: FuseOperations FH
fuseOps = defaultFuseOps { fuseOpenDirectory = openDirectory
                         , fuseReadDirectory = readDirectory
--                         , fuseGetFileStat = getFileStat

                         -- , fuseOpen               = undefined
                         -- , fuseFlush              = undefined
                         -- , fuseRead               = undefined
--                         , fuseGetFileSystemStats = getFileSystemStats

                         -- Dummies to make FUSE happy.
                         , fuseSetFileSize      = \_ _   -> return eOK
                         , fuseSetFileTimes     = \_ _ _ -> return eOK
                         , fuseSetFileMode      = \_ _   -> return eOK
                         , fuseSetOwnerAndGroup = \_ _ _ -> return eOK
                         }

------------------------------------------------------------
-- Testy bleh stuff
------------------------------------------------------------
main = do
  redis <- connect localhost defaultPort
  runFuse

go :: WithRedis m => m ()
go = do
  addFileTag "pic1.jpg" "cat"
  addFileTag "pic1.jpg" "lol"


