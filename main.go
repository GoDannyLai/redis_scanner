package main

import (
	"dannytools/ehand"
	"dannytools/logging"
	"dannytools/myredis"
	"fmt"
	"os"
	"strings"
	"sync"
)

func init() {
	g_loger.CreateNewRawLogger()
	var err error
	gCwd, err = os.Getwd()
	if err != nil {
		g_loger.WriteToLogByFieldsErrorExtramsgExit(err, "fail to get current working dir", logging.ERROR, ehand.ERR_DIR_GETCWD)
	}
}

func main() {
	// parse cmd line option
	g_conf.parseCmdOptions()

	// get redis info, and find masters and latest slaves
	gMasters, gScanNodes := GetMasterAndLatestSlavesInfo(g_conf)

	var (
		wgKey       sync.WaitGroup
		wgProcess   sync.WaitGroup
		keysChanAll map[string]chan string
		targetNodes map[string]*myredis.RedisServer = map[string]*myredis.RedisServer{}
	)

	// scan keys
	keysChanAll = ScanKeys(g_conf, gScanNodes, gMasters, &wgKey)

	// dry run, just print the target keys
	if g_conf.dryRun {
		for addr, oneChan := range keysChanAll {
			onekeyFile := GetTargetKeysFileFromAddrString(g_conf.outdir, addr)
			wgKey.Add(1)
			go PrintKeysIntoFile(onekeyFile, oneChan, &wgKey)
		}
		wgKey.Wait()
		return
	}

	if g_conf.commands == gc_command_bigkey || g_conf.commands == gc_command_dump {
		for addr := range keysChanAll {
			targetNodes[addr] = gScanNodes[addr]
		}
		g_loger.WriteToLogByFieldsNormalOnlyMsg(myredis.GetInfoStringForNodes("get key value and info from below redis:", targetNodes, gMasters), logging.INFO)
		// scan big keys or dump keys
		var keyInfoChanAll map[string]chan myredis.RedisKey = map[string]chan myredis.RedisKey{}
		for addr := range keysChanAll {
			oneNode := targetNodes[addr]
			var sampleRate uint = 0
			if !g_conf.exactlySize {
				versionGt4, err := myredis.CompareRedisVersionStr(oneNode.Version, gc_redis_version4)

				if err != nil {
					g_loger.WriteToLogByFieldsErrorExtramsg(err, fmt.Sprintf("%s, error to check if version >= %s, not going to use MEMORY USAGE to sample and get mem usage of redis keys",
						oneNode.InfoString(), gc_redis_version4), logging.ERROR)

				} else if versionGt4 < 0 {
					g_loger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("%s, version < %s, not going to use MEMORY USAGE to sample and get mem usage of redis keys",
						oneNode.InfoString(), gc_redis_version4), logging.WARNING)
				} else {
					g_loger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("%s, going to use MEMORY USAGE to sample and get mem usage of redis keys",
						oneNode.InfoString()), logging.WARNING)
					sampleRate = g_conf.sampleRate
				}
			} else {
				g_loger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("%s, going to scan and get value one by one to  get mem usage of redis keys",
					oneNode.InfoString()), logging.WARNING)
			}
			onePrintChan := make(chan myredis.RedisKey, GetChanBuf(g_conf))
			keyInfoChanAll[addr] = onePrintChan

			// print result
			wgKey.Add(1)
			oneResultFile := GetResultFile(g_conf.bigKeyOnlyKeyName, g_conf.commands, g_conf.outdir, oneNode.Addr)
			go PrintKeyInfoAndValueIntoFileNew(g_conf, oneResultFile, onePrintChan, &wgKey)

			// scan keys for bigkey or dumping
			var tis uint
			for tis = 1; tis <= g_conf.threadsEachInstance; tis++ {
				wgProcess.Add(1)
				go GetKeysValuesAndBytes(g_conf, sampleRate, oneNode, keysChanAll[addr], onePrintChan, &wgProcess)
			}

		}
		wgProcess.Wait()
		for addr := range keyInfoChanAll {
			close(keyInfoChanAll[addr])
		}

	} else if g_conf.commands == gc_command_delete {

		// delete keys
		var (
			masterAddrStr  string
			deleteType     uint8                  = gc_delete_expire
			deletedChanAll map[string]chan string = map[string]chan string{}
			infArr         []string
		)

		for addr := range keysChanAll {
			if gScanNodes[addr].IsMaster() {
				targetNodes[addr] = gScanNodes[addr]
				infArr = append(infArr, gScanNodes[addr].InfoString())
			} else {
				masterAddrStr = gScanNodes[addr].Master.AddrString()
				if _, ok := gMasters[masterAddrStr]; !ok {
					g_loger.WriteToLogByFieldsExitMsgNoErr(fmt.Sprintf("strange, as slave, but %s does not have a master", gScanNodes[addr].InfoString()),
						logging.ERROR, ehand.ERR_ERROR)
				}
				targetNodes[addr] = gMasters[masterAddrStr]
				infArr = append(infArr, myredis.GetInfoStringOnlyOneSlaveAndMaster(false, gScanNodes[addr], gMasters[masterAddrStr]))

			}
		}
		g_loger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("delete keys from below redis:\n\t%s", strings.Join(infArr, "\n\t")), logging.INFO)

		for addr := range keysChanAll {

			oneMaster := targetNodes[addr]
			deleteType = gc_delete_expire
			if g_conf.ifDirectDel {
				versionGt4, err := myredis.CompareRedisVersionStr(oneMaster.Version, gc_redis_version4)

				if err != nil {
					g_loger.WriteToLogByFieldsErrorExtramsg(err, fmt.Sprintf("%s, error to check if version >= %s, going to use del to delete keys",
						oneMaster.InfoString(), gc_redis_version4), logging.ERROR)
					deleteType = gc_delete_del

				} else if versionGt4 >= 0 {
					g_loger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("%s, version >= %s,  going to use unlink to delete keys",
						oneMaster.InfoString(), gc_redis_version4), logging.WARNING)
					deleteType = gc_delete_unlink
				} else {
					g_loger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("%s, version < %s,  going to use del to delete keys",
						oneMaster.InfoString(), gc_redis_version4), logging.WARNING)
					deleteType = gc_delete_del
				}
			}

			oneDelChan := make(chan string, GetChanBuf(g_conf))
			deletedChanAll[addr] = oneDelChan

			//print deleted keys
			oneDelFile := GetResultFile(g_conf.bigKeyOnlyKeyName, g_conf.commands, g_conf.outdir, oneMaster.Addr)
			wgKey.Add(1)
			go PrintKeysIntoFile(oneDelFile, oneDelChan, &wgKey)

			// delete keys
			var tid uint
			for tid = 1; tid <= g_conf.threadsEachInstance; tid++ {
				wgProcess.Add(1)
				go DeleteKeys(g_conf, deleteType, oneMaster, keysChanAll[addr], oneDelChan, &wgProcess)
			}

		}
		wgProcess.Wait()
		for addr := range deletedChanAll {
			close(deletedChanAll[addr])
		}
	}

	wgKey.Wait()

}
