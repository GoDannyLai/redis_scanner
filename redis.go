package main

import (
	"bufio"
	"dannytools/constvar"
	"dannytools/ehand"
	"dannytools/logging"
	"dannytools/myredis"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	//"github.com/davecgh/go-spew/spew"
	"github.com/go-redis/redis"
)

const (
	cThreadEachCpuRedis int = 32
)

var (
	//gRedisTop map[string][]string = map[string][]string{}
	gKeyFiles  map[string]string = map[string]string{}
	gCwd       string
	gTpRedis   string = fmt.Sprintf("%d", gc_type_redis)
	gTpCluster string = fmt.Sprintf("%d", gc_type_cluster)
)

func CheckRedisInfo(cfg *CmdConf) (map[string]*myredis.RedisServer, map[string][]myredis.RedisAddr) {
	var (
		redisServers map[string]*myredis.RedisServer = map[string]*myredis.RedisServer{}
		err          error

		redisTypes map[string][]myredis.RedisAddr = map[string][]myredis.RedisAddr{gTpRedis: []myredis.RedisAddr{}, gTpCluster: []myredis.RedisAddr{}}
		tpStr      string
	)
	g_loger.WriteToLogByFieldsNormalOnlyMsg("start to check redis info", logging.INFO)
	for _, addr := range cfg.redisAddrs {
		rServer := &myredis.RedisServer{Addr: addr}
		err = rServer.GetAndSetRedisInfo()
		if err != nil {
			g_loger.WriteToLogByFieldsErrorExtramsgExit(err, "fail to get redis info "+addr.AddrString(), logging.ERROR, ehand.ERR_REDIS_INFO)
		}
		/*
			if cfg.redisType == gc_type_redis && rServer.IsCluster && cfg.commands == gc_command_delete && !cfg.dryRun {
				g_loger.WriteToLogByFieldsExitMsgNoErr(fmt.Sprintf("-rt is set to redis but %s is actually cluster, in this case , %s without dry run is not allowed",
					addr.AddrString(), gc_command_delete), logging.ERROR, ehand.ERR_ERROR)
			}
		*/
		if rServer.IsCluster {
			tpStr = gTpCluster
		} else {
			tpStr = gTpRedis
		}

		redisTypes[tpStr] = append(redisTypes[tpStr], addr)
		if rServer.Role != myredis.CRoleMaster && !rServer.ReplStatus.IfOnline {
			g_loger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("%s, but replication is stopped", rServer.InfoString()), logging.WARNING)
		}
		redisServers[addr.AddrString()] = rServer
		g_loger.WriteToLogByFieldsNormalOnlyMsg(rServer.InfoString(), logging.INFO)

	}

	return redisServers, redisTypes
}

func CheckAndSetRedisInfoForAllNodes(cfg *CmdConf) map[string]*myredis.RedisServer {

	var (
		rServers map[string]*myredis.RedisServer
		rTypes   map[string][]myredis.RedisAddr
		ok       bool

		addrStr string
	)
	rServers, rTypes = CheckRedisInfo(cfg)
	//spew.Dump(rTypes)
	// scan keys from redis and redis type is not specfied, for actual cluster, we need to scan keys from all latest slave nodes
	//if len(rTypes[gTpCluster]) > 0 && cfg.keyfileDir != "" && cfg.redisType != gc_type_redis {

	// for cluster, need to get all nodes info
	if cfg.ifDiscoveryClusterNodes && len(rTypes[gTpCluster]) > 0 {

		for _, addr := range rTypes[gTpCluster] {
			addrStr = addr.AddrString()
			g_loger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("find peer cluster nodes of %s", addrStr), logging.INFO)
			clusterClient, err := addr.CreateConPoolCluster(false, cThreadEachCpuRedis)
			if err != nil {
				g_loger.WriteToLogByFieldsErrorExtramsgExit(err, "fail to connect to redis "+addrStr, logging.ERROR, ehand.ERR_REDIS_CONNECT)
			}
			nodeAddr, err := myredis.GetClusterNodesAddr(clusterClient)
			if err != nil {
				g_loger.WriteToLogByFieldsErrorExtramsgExit(err, "fail to get redis cluster nodes "+addrStr, logging.ERROR, ehand.ERR_REDIS_CLUSTER_NODES)
			}
			for addrStr = range nodeAddr {
				if _, ok = rServers[addrStr]; ok {
					continue
				}
				oneServer := &myredis.RedisServer{Addr: nodeAddr[addrStr]}
				err = oneServer.GetAndSetRedisInfo()
				if err != nil {
					g_loger.WriteToLogByFieldsErrorExtramsgExit(err, "fail to get redis info "+addrStr, logging.ERROR, ehand.ERR_REDIS_INFO)
				}
				rServers[addrStr] = oneServer
			}
		}

	}
	return rServers
}

func GetAllMasterInfo(rServers map[string]*myredis.RedisServer) map[string]*myredis.RedisServer {
	var (
		masters map[string]*myredis.RedisServer = map[string]*myredis.RedisServer{}
		err     error
		ok      bool
	)
	g_loger.WriteToLogByFieldsNormalOnlyMsg("start to get redis masters", logging.INFO)
	for ipPort, rs := range rServers {

		if rs.Role == myredis.CRoleMaster {
			masters[ipPort] = rs
			//gRedisTop[ipPort] = rs.GetAllNodesAddrStr()
			continue
		} else {
			if _, ok = masters[rs.Master.AddrString()]; ok {
				continue
			}
			if _, ok = rServers[rs.Master.AddrString()]; ok {
				masters[rs.Master.AddrString()] = rServers[rs.Master.AddrString()]
				continue
			}
		}

		g_loger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("%s is slave, check its master %s info", rs.Addr.AddrString(), rs.Master.AddrString()), logging.INFO)
		mServer := &myredis.RedisServer{Addr: rs.Master}
		err = mServer.GetAndSetRedisInfo()
		if err != nil {
			g_loger.WriteToLogByFieldsErrorExtramsgExit(err, "error to check redis info of "+rs.Master.AddrString(), logging.ERROR, ehand.ERR_ERROR)
		}
		//gRedisTop[ipPort] = mServer.GetAllNodesAddrStr()

		masters[rs.Master.AddrString()] = mServer
	}
	return masters
}

func GetAllNodesForReadOnly(cfg *CmdConf, masters map[string]*myredis.RedisServer) map[string]myredis.RedisAddr {
	var (
		slaves map[string]myredis.RedisAddr = map[string]myredis.RedisAddr{}
	)
	g_loger.WriteToLogByFieldsNormalOnlyMsg("start to get latest slave or master to scan keys", logging.INFO)
	for ipPort, rs := range masters {
		rAddr, err := rs.GetLatestSlave()
		if err != nil {
			if cfg.forceAddr {
				g_loger.WriteToLogByFieldsErrorExtramsg(err, fmt.Sprintf("%s has no suitable slave to scan keys, but you specify -F, so use itself instead", ipPort), logging.ERROR)
				slaves[ipPort] = rs.Addr
			} else {
				g_loger.WriteToLogByFieldsErrorExtramsgExit(err, fmt.Sprintf("%s has no suitable slave to scan keys, you may specify -F to scan keys from master", ipPort), logging.ERROR, ehand.ERR_ERROR)
			}
		} else {
			slaves[rAddr.AddrString()] = rAddr
			g_loger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("use latest slave %s to scan keys", rAddr.AddrString()), logging.INFO)
		}
	}
	return slaves
}

// return master servers and scan node servers

func GetMasterAndLatestSlavesInfo(cfg *CmdConf) (map[string]*myredis.RedisServer, map[string]*myredis.RedisServer) {
	var (
		//scanArr   []string
		//infStr    string
		ifOk      bool
		oneServer *myredis.RedisServer
		err       error
		ok        bool
	)
	servers := CheckAndSetRedisInfoForAllNodes(cfg)
	masters := GetAllMasterInfo(servers)
	scanNodeAddrs := GetAllNodesForReadOnly(cfg, masters)
	scanNodeServers := map[string]*myredis.RedisServer{}

	for ipPort, addr := range scanNodeAddrs {
		ifOk = false
		if _, ok = servers[ipPort]; ok {
			oneServer = servers[ipPort]
			ifOk = true
		} else if _, ok = masters[ipPort]; ok {
			oneServer = masters[ipPort]
			ifOk = true

		} else {
			g_loger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("strange, %s does not have redis info, get and check it", addr.AddrString()), logging.WARNING)
			oneServer = &myredis.RedisServer{Addr: addr}
			err = oneServer.GetAndSetRedisInfo()
			if err != nil {
				g_loger.WriteToLogByFieldsErrorExtramsgExit(err, fmt.Sprintf("error to check redis info of %s", addr.AddrString()),
					logging.ERROR, ehand.ERR_ERROR)
				ifOk = false
			} else {
				ifOk = true
			}
		}
		if ifOk {
			scanNodeServers[ipPort] = oneServer
			/*
				infStr = oneServer.InfoString()
				if oneServer.IsSlave() {
					infStr += " --> " + servers[oneServer.Master.AddrString()].InfoString()
				}
				scanArr = append(scanArr, infStr)
			*/
		}
	}

	//g_loger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("below nodes are candidate nodes to scan keys:\n\t%s", strings.Join(scanArr, "\n\t")), logging.INFO)

	return masters, scanNodeServers
}

func ScanKeysFromOneKeyFile(cfg *CmdConf, keyFile string, keysChan chan string, wg *sync.WaitGroup) {
	//wg.Add(1)
	defer wg.Done()
	defer close(keysChan)

	var (
		ifReg bool = false
		line  string
		err   error
	)

	g_loger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("start thread to read keys from file %s", keyFile), logging.INFO)

	if cfg.patternReg.String() != "" {
		ifReg = true
	}

	FH, err := os.Open(keyFile)
	if err != nil {
		g_loger.WriteToLogByFieldsErrorExtramsgExit(err, fmt.Sprintf("fail to open key file %s", keyFile), logging.ERROR, ehand.ERR_FILE_OPEN)
	}
	defer FH.Close()
	bufFH := bufio.NewReader(FH)

	for {
		line, err = bufFH.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			} else {
				g_loger.WriteToLogByFieldsErrorExtramsgExit(err, "error to read key from file "+keyFile, logging.ERROR, ehand.ERR_FILE_READ)
			}
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		if ifReg {
			if cfg.patternReg.MatchString(line) {
				keysChan <- line
			}
		} else {
			keysChan <- line
		}

	}

	g_loger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("exit thread to read keys from file %s", keyFile), logging.INFO)
}

func ScanKeysFromOneRedis(cfg *CmdConf, redisInfoStr string, client *redis.Client, keysChan chan string, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(keysChan)
	defer client.Close()

	g_loger.WriteToLogByFieldsNormalOnlyMsg("start thread to scan keys from redis "+redisInfoStr, logging.INFO)

	err := myredis.ScanRedisKeys(client, int64(cfg.elementScanBatch), cfg.patternReg, int(cfg.keyBatch), int(cfg.keyInterval), keysChan)
	if err != nil {
		g_loger.WriteToLogByFieldsErrorExtramsgExit(err, "error to scan keys from redis "+redisInfoStr, logging.ERROR, ehand.ERR_REDIS_SCAN)
	}

	g_loger.WriteToLogByFieldsNormalOnlyMsg("exit thread to scan keys from redis "+redisInfoStr, logging.INFO)

}

func ScanKeys(cfg *CmdConf, rServers map[string]*myredis.RedisServer, masters map[string]*myredis.RedisServer, wg *sync.WaitGroup) map[string]chan string {

	var (
		reChans  map[string]chan string = map[string]chan string{}
		chanBuf  int
		database int = int(cfg.database)
		addr     string
		filesArr []string
	)

	chanBuf = GetChanBuf(cfg)

	if cfg.ifKeyFiles {
		for addr, f := range cfg.keyFiles {
			filesArr = append(filesArr, fmt.Sprintf("%s <=> %s", f, myredis.GetInfoStringOnlyOneNode(rServers[addr], masters)))
		}
		g_loger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("read keys from below files:\n\t%s", strings.Join(filesArr, "\n\t")), logging.INFO)
		for addr = range cfg.keyFiles {
			oneChan := make(chan string, chanBuf)
			reChans[addr] = oneChan
			oneKeyFile := cfg.keyFiles[addr]
			wg.Add(1)
			go ScanKeysFromOneKeyFile(cfg, oneKeyFile, oneChan, wg)
		}
	} else {
		g_loger.WriteToLogByFieldsNormalOnlyMsg(myredis.GetInfoStringForNodes("scan keys from below redis:", rServers, masters), logging.INFO)
		for addr = range rServers {
			oneClient, err := rServers[addr].Addr.CreateConPoolRedis(database, cThreadEachCpuRedis)
			if err != nil {
				g_loger.WriteToLogByFieldsErrorExtramsgExit(err, "error to connect redis "+rServers[addr].InfoString(),
					logging.ERROR, ehand.ERR_REDIS_CONNECT)
			}
			oneChan := make(chan string, chanBuf)
			reChans[addr] = oneChan
			wg.Add(1)
			go ScanKeysFromOneRedis(cfg, rServers[addr].InfoString(), oneClient, oneChan, wg)
		}
	}
	/*
		for addr = range rServers {
			oneChan := make(chan string, chanBuf)
			reChans[addr] = oneChan
			if cfg.ifKeyFiles {
				oneKeyFile := cfg.keyFiles[addr]
				wg.Add(1)
				go ScanKeysFromOneKeyFile(cfg, oneKeyFile, oneChan, wg)
			} else {
				// addr may not be the same as rServers[addr].Addr.AddrString()? should not
				actaulAddr = rServers[addr].Addr.AddrString()
				oneClient, err := rServers[addr].Addr.CreateConPoolRedis(database, cThreadEachCpuRedis)
				if err != nil {
					g_loger.WriteToLogByFieldsErrorExtramsgExit(err, "error to connect redis "+rServers[addr].InfoString(),
						logging.ERROR, ehand.ERR_REDIS_CONNECT)
				}
				wg.Add(1)
				go ScanKeysFromOneRedis(cfg, actaulAddr, oneClient, oneChan, wg)
			}

		}
	*/
	return reChans

}

// sampleRate: for redis4.0

func GetKeysValuesAndBytes(cfg *CmdConf, sampleRate uint, rserver *myredis.RedisServer, keysChan chan string, printChan chan myredis.RedisKey, wg *sync.WaitGroup) {
	defer wg.Done()

	var (
		err             error
		key             string
		keyArr          []string
		keyTypes        map[string]string
		alreadyCntPipe  int                 = 0
		alreadyCntKey   int                 = 0
		keyTypeArr      map[string][]string = map[string][]string{}
		k               string
		t               string
		ok              bool
		pipeSize        int = int(cfg.pipeLineSize)
		redisKeys       []myredis.RedisKey
		minBytes        int64         = int64(cfg.sizeLimit)
		rangeBatch      int64         = int64(cfg.elementScanBatch)
		elementBatch    int64         = int64(cfg.elementBatch)
		elementInterval time.Duration = time.Duration(cfg.elementInterval) * time.Microsecond
		keyInterval     time.Duration = time.Duration(cfg.keyInterval) * time.Microsecond
		ifSleep         bool          = false
		ifBytes         bool          = false
		ifValue         bool          = false
		i               int
		keyBatch        int    = int(cfg.keyBatch)
		redisInfo       string = rserver.InfoString()
	)

	g_loger.WriteToLogByFieldsNormalOnlyMsg("start thread GetKeysValuesAndBytes from "+redisInfo, logging.INFO)

	client, err := rserver.Addr.CreateRedisOrClusterConPool(false, false, int(cfg.database), cThreadEachCpuRedis)
	defer client.Close()
	if err != nil {
		g_loger.WriteToLogByFieldsErrorExtramsgExit(err, "error to connect to redis "+redisInfo, logging.ERROR, ehand.ERR_REDIS_CONNECT)
	}
	if rserver.IsCluster {
		_, err = client.Redis.ReadOnly().Result()
		if err != nil {
			g_loger.WriteToLogByFieldsErrorExtramsgExit(err, "error to set redis connection read only "+redisInfo, logging.ERROR, ehand.ERR_REDIS_CONNECT)
		} else {
			g_loger.WriteToLogByFieldsNormalOnlyMsg("successfully set redis connection read only "+redisInfo, logging.INFO)
		}
	}

	if cfg.elementInterval > 0 {
		ifSleep = true
	}
	if cfg.commands == gc_command_dump || cfg.commands == gc_command_bigkey {
		ifBytes = true
	}

	if cfg.commands == gc_command_dump {
		ifValue = true
	}

	for key = range keysChan {
		keyArr = append(keyArr, key)
		alreadyCntPipe++
		if alreadyCntPipe < pipeSize {
			continue
		} else {
			alreadyCntPipe = 0
			keyTypes, err = client.GetKeysTypePipe(keyArr)
			keyArr = nil
			if err != nil {
				g_loger.WriteToLogByFieldsErrorExtramsgExit(err, "error to get key type", logging.ERROR, ehand.ERR_REDIS_TYPE)
			}
			for k, t = range keyTypes {
				if _, ok = keyTypeArr[t]; ok {
					keyTypeArr[t] = append(keyTypeArr[t], k)
				} else {
					keyTypeArr[t] = []string{k}
				}
			}

		}
		for t = range keyTypeArr {
			if len(keyTypeArr[t]) < pipeSize {
				continue
			}
			//fmt.Printf("process keys: %s\n", strings.Join(keyTypeArr[t], ", "))
			redisKeys, err = client.GetKeysValueAdaptive(keyTypeArr[t], t, cfg.database, minBytes,
				constvar.DATETIME_FORMAT_NOSPACE, cfg.minTtlSeconds, cfg.maxTtlSeconds,
				sampleRate, gc_sample_min, rangeBatch, elementBatch, elementInterval, ifSleep, ifBytes, ifValue)
			if err != nil {
				g_loger.WriteToLogByFieldsErrorExtramsgExit(err, "error to get key value and bytes", logging.ERROR, ehand.ERR_REDIS_GET_KEY_VALUE)
			}
			for i = range redisKeys {
				printChan <- redisKeys[i]
			}
			redisKeys = nil
			if cfg.keyInterval > 0 {
				alreadyCntKey += len(keyTypeArr[t])
			}
			delete(keyTypeArr, t)
			if cfg.keyInterval > 0 && alreadyCntKey > keyBatch {
				alreadyCntKey = 0
				time.Sleep(keyInterval)
			}

		}

	}

	if len(keyArr) > 0 {
		keyTypes, err = client.GetKeysTypePipe(keyArr)
		keyArr = nil
		if err != nil {
			g_loger.WriteToLogByFieldsErrorExtramsgExit(err, "error to get key type", logging.ERROR, ehand.ERR_REDIS_TYPE)
		}
		for k, t = range keyTypes {
			if _, ok = keyTypeArr[t]; ok {
				keyTypeArr[t] = append(keyTypeArr[t], k)
			} else {
				keyTypeArr[t] = []string{k}
			}
		}
	}

	for t = range keyTypeArr {
		//fmt.Printf("process keys: %s\n", strings.Join(keyTypeArr[t], ", "))
		if len(keyTypeArr[t]) == 0 {
			continue
		}
		redisKeys, err = client.GetKeysValueAdaptive(keyTypeArr[t], t, cfg.database, minBytes,
			constvar.DATETIME_FORMAT_NOSPACE, cfg.minTtlSeconds, cfg.maxTtlSeconds,
			sampleRate, gc_sample_min, rangeBatch, elementBatch, elementInterval, ifSleep, ifBytes, ifValue)
		if err != nil {
			g_loger.WriteToLogByFieldsErrorExtramsgExit(err, "error to get key value and bytes", logging.ERROR, ehand.ERR_REDIS_GET_KEY_VALUE)
		}
		for i = range redisKeys {
			printChan <- redisKeys[i]
		}
		redisKeys = nil
		delete(keyTypeArr, t)
	}

	g_loger.WriteToLogByFieldsNormalOnlyMsg("exit thread GetKeysValuesAndBytes from "+redisInfo, logging.INFO)
}

// deleteType: 0=delete, 1=unlink(for redis4.0), else=expire it

func DeleteKeys(cfg *CmdConf, deleteType uint8, oneMaster *myredis.RedisServer, keysChan chan string, deletedKeysChan chan string, wg *sync.WaitGroup) {
	defer wg.Done()

	var (
		err            error
		keyArr         []string
		alreadyCntPipe int = 0
		alreadyCntKey  int = 0
		keyBatch       int = int(cfg.keyBatch)
		pipeSize       int = int(cfg.pipeLineSize)
		maxExp         int = int(cfg.delKeyExpireMax)
		k              string
		sleepInterval  time.Duration = time.Duration(cfg.keyInterval) * time.Microsecond
		addStr                       = oneMaster.InfoString()
	)

	g_loger.WriteToLogByFieldsNormalOnlyMsg("start thread DeleteKeys from "+addStr, logging.INFO)

	client, err := oneMaster.CreateGenClientPool(false, int(g_conf.database), cThreadEachCpuRedis)
	defer client.Close()
	if err != nil {
		g_loger.WriteToLogByFieldsErrorExtramsgExit(err, "error to connect to "+oneMaster.InfoString(), logging.ERROR, ehand.ERR_REDIS_CONNECT)
	}

	for key := range keysChan {
		keyArr = append(keyArr, key)
		alreadyCntPipe++
		if alreadyCntPipe >= pipeSize {
			if cfg.keyInterval > 0 {
				alreadyCntKey += alreadyCntPipe
			}
			alreadyCntPipe = 0
			switch deleteType {
			case gc_delete_del:
				err = client.DeleteKeysByDirectlyDelIt(keyArr)
			case gc_delete_unlink:
				err = client.DeleteKeysByUnlinkIt(keyArr)
			default:
				err = client.DeleteKeysByExpireItPipe(keyArr, time.Duration(rand.Intn(maxExp)+1)*time.Second)
			}
			if err != nil {
				g_loger.WriteToLogByFieldsErrorExtramsgExit(err, "error to delete key", logging.ERROR, ehand.ERR_REDIS_DELETE)
			}
			for _, k = range keyArr {
				deletedKeysChan <- k
			}
			keyArr = nil
			if cfg.keyInterval > 0 && alreadyCntKey >= keyBatch {
				alreadyCntKey = 0
				time.Sleep(sleepInterval)
			}
		}

	}
	if len(keyArr) > 0 {
		switch deleteType {
		case gc_delete_del:
			err = client.DeleteKeysByDirectlyDelIt(keyArr)
		case gc_delete_unlink:
			err = client.DeleteKeysByUnlinkIt(keyArr)
		default:
			err = client.DeleteKeysByExpireItPipe(keyArr, time.Duration(rand.Intn(maxExp)+1)*time.Second)
		}
		if err != nil {
			g_loger.WriteToLogByFieldsErrorExtramsgExit(err, "error to delete key", logging.ERROR, ehand.ERR_REDIS_DELETE)
		}
		for _, k = range keyArr {
			deletedKeysChan <- k
		}
	}

	g_loger.WriteToLogByFieldsNormalOnlyMsg("exit thread DeleteKeys from "+addStr, logging.INFO)

}

/*
write keys to file, one key one line
*/
func PrintKeysIntoFile(keyFile string, keysChan chan string, wg *sync.WaitGroup) {
	defer wg.Done()
	var (
		err error
	)

	g_loger.WriteToLogByFieldsNormalOnlyMsg("start thread of write keys to file "+keyFile, logging.INFO)

	fh, err := os.OpenFile(keyFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0744)
	if fh != nil {
		defer fh.Close()
	}
	if err != nil {
		g_loger.WriteToLogByFieldsErrorExtramsgExit(err, "fail to open file "+keyFile, logging.ERROR, ehand.ERR_FILE_OPEN)
	}

	for key := range keysChan {
		_, err = fh.WriteString(key + "\n")
		if err != nil {
			g_loger.WriteToLogByFieldsErrorExtramsgExit(err, fmt.Sprintf("fail to write key %s into file %s", key, keyFile),
				logging.ERROR, ehand.ERR_FILE_WRITE)
		}
	}
	g_loger.WriteToLogByFieldsNormalOnlyMsg("exit thread of write keys to file"+keyFile, logging.INFO)

}

/*
dump key value or big key info into file
*/

func PrintKeyInfoAndValueIntoFileNew(cfg *CmdConf, reFile string, printChan chan myredis.RedisKey, wg *sync.WaitGroup) {
	defer wg.Done()

	var (
		err           error
		str           string
		oneVal        myredis.RedisKey
		ifBigKey      bool = false
		needSortKeyes []myredis.RedisKey
		printCnt      int
		sortCnt       int = 0
		outLimit      int = int(cfg.limit)
		sortSize      int = 2 * outLimit
	)
	if sortSize < 100 {
		sortSize = 100
	} else if sortSize >= 500 {
		sortSize = 500
	}

	g_loger.WriteToLogByFieldsNormalOnlyMsg("start thread of writing result to file "+reFile, logging.INFO)
	if cfg.commands == "bigkey" {
		ifBigKey = true
	}
	fh, err := os.OpenFile(reFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0744)
	if fh != nil {
		defer fh.Close()
	}
	if err != nil {
		g_loger.WriteToLogByFieldsErrorExtramsgExit(err, "fail to open file "+reFile, logging.ERROR, ehand.ERR_FILE_OPEN)
	}

	if ifBigKey && !cfg.bigKeyOnlyKeyName {
		fh.WriteString(myredis.GetRedisBigkeyHeader())
	}

	if ifBigKey && cfg.ifSortedKey && !cfg.bigKeyOnlyKeyName {
		//bigkey with sorting

		for oneVal = range printChan {
			sortCnt++
			needSortKeyes = append(needSortKeyes, oneVal)
			if sortCnt >= sortSize {
				sortCnt = 0
				if len(needSortKeyes) > outLimit {
					sort.SliceStable(needSortKeyes, func(i, j int) bool { return needSortKeyes[i].Bytes > needSortKeyes[j].Bytes })
					needSortKeyes = needSortKeyes[0:outLimit]
				}

			}
		}
		sort.SliceStable(needSortKeyes, func(i, j int) bool { return needSortKeyes[i].Bytes > needSortKeyes[j].Bytes })
		if cfg.limit > 0 && len(needSortKeyes) > outLimit {
			printCnt = outLimit
		} else {
			printCnt = len(needSortKeyes)
		}

		for _, oneVal = range needSortKeyes[0:printCnt] {

			str, err = oneVal.GetRedisKeyPrintLine(ifBigKey, cfg.pretty, g_json_indent)
			if err != nil {
				g_loger.WriteToLogByFieldsErrorExtramsgExit(err, fmt.Sprintf("fail to get print string for key %s", oneVal.Key),
					logging.ERROR, ehand.ERR_JSON_MARSHAL)
			}
			_, err = fh.WriteString(str)
			if err != nil {
				g_loger.WriteToLogByFieldsErrorExtramsgExit(err, fmt.Sprintf("fail to write key info of %s into file %s: %s", oneVal.Key, reFile, str),
					logging.ERROR, ehand.ERR_FILE_WRITE)
			}

		}

	} else {
		//dump key or bigkey without sorting
		for oneVal = range printChan {
			if ifBigKey && cfg.bigKeyOnlyKeyName {
				str = oneVal.Key + "\n"
			} else {
				str, err = oneVal.GetRedisKeyPrintLine(ifBigKey, cfg.pretty, g_json_indent)

				if err != nil {
					g_loger.WriteToLogByFieldsErrorExtramsgExit(err, fmt.Sprintf("fail to get print string for key %s", oneVal.Key),
						logging.ERROR, ehand.ERR_JSON_MARSHAL)
				}
				// for pretty dump
				if !ifBigKey && cfg.pretty {
					_, err = fh.WriteString(gc_json_line_seperator)
					if err != nil {
						g_loger.WriteToLogByFieldsErrorExtramsgExit(err, fmt.Sprintf("fail to write %s into file %s for key",
							gc_json_line_seperator, reFile, oneVal.Key),
							logging.ERROR, ehand.ERR_FILE_WRITE)
					}

				}

				if !ifBigKey {
					str += "\n"
				}
			}
			_, err = fh.WriteString(str)

			if err != nil {
				g_loger.WriteToLogByFieldsErrorExtramsgExit(err, fmt.Sprintf("fail to write key info of %s into file %s: %s", oneVal.Key, reFile, str),
					logging.ERROR, ehand.ERR_FILE_WRITE)
			}

		}
	}

	g_loger.WriteToLogByFieldsNormalOnlyMsg("exit thread of writing result to file "+reFile, logging.INFO)

}
