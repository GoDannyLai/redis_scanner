package main

import (
	"dannytools/constvar"
	"dannytools/ehand"
	"dannytools/logging"
	"dannytools/myredis"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	kitsFile "github.com/toolkits/file"
	kitsSlice "github.com/toolkits/slice"
)

const (
	gc_version       string = "redis_scanner V2.0 --By laijunshou@gmail.com"
	gc_usage_comment string = "safely dump keys, delete keys or find bigkeys in redis or redis cluster"

	//gc_scankeys_batch    int64 = 200
	//gc_scanelement_batch int64 = 1000
	//gc_delete_expire_seconds_max uint  = 10

	gc_json_line_seperator = "\n#one key#\n"

	gc_result_json_file   = "redis_dump.json"
	gc_result_bigkey_file = "redis_bigkeys.csv"
	gc_dryrun_keys_file   = "redis_target_keys.txt"
	gc_deleted_keys_file  = "deleted_keys.txt"

	gc_type_redis        uint = 1
	gc_type_cluster      uint = 2
	gc_type_autodiscover uint = 0

	gc_command_dump   string = "dump"
	gc_command_delete string = "delete"
	gc_command_bigkey string = "bigkey"

	gc_sample_min uint = 5

	gc_redis_version4 string = "4.0.0"

	//deleteType: 0=delete, 1=unlink(for redis4.0), else=expire it
	gc_delete_del    uint8 = 0
	gc_delete_unlink uint8 = 1
	gc_delete_expire uint8 = 2
)

var (
	g_json_indent string = constvar.JSON_INDENT_TAB

	g_validCmds       []string = []string{gc_command_dump, gc_command_delete, gc_command_bigkey}
	g_validRedisTypes []uint   = []uint{gc_type_autodiscover, gc_type_redis, gc_type_cluster} //1=redis, 2=cluster, 0=auto-discover

	g_loger *logging.MyLog = &logging.MyLog{}

	g_conf *CmdConf = &CmdConf{}

	g_default_uints map[string]uint = map[string]uint{
		"port":             6379,
		"limit":            100,
		"database":         0,
		"redisType":        0,
		"keyBatch":         500,
		"keyInterval":      10,
		"elementBatch":     1000,
		"elementInterval":  10,
		"elementScanBatch": 200,
		"threadEachRedis":  1,
		"pipelineSize":     10,
		"sampleRate":       10,
		"sizeLimit":        3072,
		"expireSecs":       10,
	}
)

type CmdConf struct {
	host string
	port uint

	addrFile   string
	redisAddrs []myredis.RedisAddr
	//ifAddrFile bool

	forceAddr bool

	passwd   string
	database uint

	ifDiscoveryClusterNodes bool

	commands          string
	bigKeyOnlyKeyName bool
	pattern           string
	patternReg        *regexp.Regexp
	minTtlSeconds     int
	maxTtlSeconds     int
	keyfileDir        string
	keyFiles          map[string]string
	ifKeyFiles        bool
	dryRun            bool

	outdir string

	keyBatch         uint
	keyInterval      uint
	elementBatch     uint
	elementInterval  uint
	elementScanBatch uint

	threadsEachInstance uint
	pipeLineSize        uint

	exactlySize bool
	sampleRate  uint

	pretty bool

	sizeLimit uint

	limit uint

	ifSortedKey bool

	delKeyExpireMax uint

	ifDirectDel bool

	version bool
}

func (this *CmdConf) parseCmdOptions() {
	flag.Usage = func() {
		this.printHelpMsg()
	}

	flag.StringVar(&this.host, "H", "127.0.0.1", "server host, default 127.0.0.1")
	flag.UintVar(&this.port, "P", g_default_uints["port"], fmt.Sprintf("server  port, default %d", g_default_uints["port"]))
	flag.StringVar(&this.addrFile, "af", "", "read redis addr from this file, ip:port for each line")
	flag.BoolVar(&this.forceAddr, "F", false, "if no suitable slave to scan|dump keys, use master to scan|dump keys")

	flag.StringVar(&this.passwd, "p", "", "password to use when connecting to the server")
	flag.UintVar(&this.database, "n", g_default_uints["database"], fmt.Sprintf("database number, default %d", g_default_uints["database"]))

	flag.BoolVar(&this.ifDiscoveryClusterNodes, "dc", true, "if automatically find all node of cluster to scan/delete keys. default true")

	flag.BoolVar(&this.dryRun, "dr", false, fmt.Sprintf("dry run, default false. Do not actually dump, delete or find big keys, Only write target keys to file %s. -si doesnot work in this work", gc_dryrun_keys_file))
	flag.StringVar(&this.commands, "w", gc_command_bigkey, fmt.Sprintf("work type, default %s. valid option is one of %s", gc_command_bigkey, strings.Join(g_validCmds, ",")))
	flag.BoolVar(&this.bigKeyOnlyKeyName, "bn", false, fmt.Sprintf("only output key name for -w %s, default false. when true, disable -st", gc_command_bigkey))

	flag.StringVar(&this.pattern, "k", "", "regular expression, only process keys match this. default processing all keys")
	flag.IntVar(&this.minTtlSeconds, "mint", 0, "only care keys whose ttl >= -mint, in seconds, default 0.\n\t1)-mint==-1 and -maxt==-1: only keys not set expired time.\n\t2)-mint==0 and -maxt==0: any key.\n\t3) -mint > 0: only keys whose ttl >= -mint. \n\t4)-maxt > 0: only keys whose ttl <= -maxt")
	flag.IntVar(&this.maxTtlSeconds, "maxt", 0, "only care keys whose ttl <= -maxt, in seconds, default 0.\n\t1)-mint==-1 and -maxt==-1: only keys not set expired time.\n\t2)-mint==0 and -maxt==0: any key.\n\t3) -mint > 0: only keys whose ttl >= -mint. \n\t4)-maxt > 0: only keys whose ttl <= -maxt")
	flag.StringVar(&this.keyfileDir, "kd", "", "only read keys from files whose name is like target_keys_ip_port.txt in the directory, the ip and port should be specify by (-H and -P) or -af")

	flag.StringVar(&this.outdir, "o", "", "output result to this dir. default current working dir")

	flag.UintVar(&this.keyBatch, "kc", g_default_uints["keyBatch"],
		fmt.Sprintf("sleep after scanning|processing this count of keys. default %d", g_default_uints["keyBatch"]))
	flag.UintVar(&this.keyInterval, "ki", g_default_uints["keyInterval"],
		fmt.Sprintf("sleep for the time(microseconds), after scanning|processing -kc count of keys. default %d, no sleep if 0",
			g_default_uints["keyInterval"]))

	flag.UintVar(&this.elementBatch, "ec", g_default_uints["elementBatch"],
		fmt.Sprintf("sleep after scanning|processing this count of elements of one key. default %d",
			g_default_uints["elementBatch"]))
	flag.UintVar(&this.elementInterval, "ei", g_default_uints["elementInterval"],
		fmt.Sprintf("sleep for the time(microseconds), after scanning|processing -ec count of elements of one key. default 2, no sleep if %d",
			g_default_uints["elementInterval"]))
	flag.UintVar(&this.elementScanBatch, "es", g_default_uints["elementScanBatch"],
		fmt.Sprintf("the number of key or element to scan each command, default %d", g_default_uints["elementScanBatch"]))

	flag.UintVar(&this.threadsEachInstance, "ti", g_default_uints["threadEachRedis"],
		fmt.Sprintf("number of theads to process keys of on redis instance, default %d", g_default_uints["threadEachRedis"]))
	flag.UintVar(&this.pipeLineSize, "ps", g_default_uints["pipelineSize"],
		fmt.Sprintf("the number of query makes a pipe line. default %d", g_default_uints["pipelineSize"]))
	flag.BoolVar(&this.exactlySize, "ts", false, "if true, get key size by scanning all elements and summing. if false,  for redis 4.0, use MEMORY USAGE to get size of key. default false")
	flag.UintVar(&this.sampleRate, "sr", g_default_uints["sampleRate"],
		fmt.Sprintf("for redis 4.0, sample rate of MEMORY USAGE to get size of key, range 0~100, 0 and 100 is the same. default %d, that is %d%%",
			g_default_uints["sampleRate"], g_default_uints["sampleRate"]))

	flag.BoolVar(&this.pretty, "b", false, "output pretty json format. default false, that is, the compact json format")

	flag.UintVar(&this.sizeLimit, "si", g_default_uints["sizeLimit"],
		fmt.Sprintf("for -w=dump|bigkey, only process keys whose size >= -si(bytes), default %d", g_default_uints["sizeLimit"]))

	flag.UintVar(&this.limit, "li", g_default_uints["limit"],
		fmt.Sprintf("for -w=bigkey and work with -st , only output the biggest -li keys, valid from 1~1000,  default %d", g_default_uints["limit"]))
	flag.BoolVar(&this.ifSortedKey, "st", false, "for -w=bigkey, sort result by size of key, default false")

	flag.UintVar(&this.delKeyExpireMax, "ex", g_default_uints["expireSecs"],
		fmt.Sprintf("for -w=delete, delete key by setting expiration to randon [1,-ex] seconds. default %d", g_default_uints["expireSecs"]))

	flag.BoolVar(&this.ifDirectDel, "dd", false, "for -w=delete, directly delete keys by del(redis<4.0) or unlink(redis >=4.0). default false")

	flag.BoolVar(&this.version, "v", false, "print version and exits")

	flag.Parse()

	if this.version {
		fmt.Printf("\n%s\n\t%s\n\n", gc_version, gc_usage_comment)
		os.Exit(0)
	}

	var err error

	if !kitsSlice.ContainsString(g_validCmds, this.commands) {
		g_loger.WriteToLogByFieldsErrorExit(fmt.Errorf("-w %s is invalid, valid option is one of %s",
			this.commands, strings.Join(g_validCmds, ",")),
			logging.ERROR, ehand.ERR_INVALID_OPTION)
	}

	if this.commands != gc_command_bigkey {
		this.bigKeyOnlyKeyName = false
	}

	this.patternReg, err = regexp.Compile(this.pattern)
	if err != nil {
		g_loger.WriteToLogByFieldsErrorExit(fmt.Errorf("invalid regular expression: -k %s", this.pattern),
			logging.ERROR, ehand.ERR_INVALID_OPTION)
	}
	//fmt.Println(this.patternReg.String())

	if this.addrFile != "" {
		//this.ifAddrFile = true
		if !kitsFile.IsFile(this.addrFile) {
			g_loger.WriteToLogByFieldsErrorExit(fmt.Errorf("-af %s is not a file nor exists", this.addrFile),
				logging.ERROR, ehand.ERR_FILE_NOT_EXISTS)
		} else {
			this.redisAddrs, err = myredis.ParseAddrFromFile(this.addrFile)
			if err != nil {
				g_loger.WriteToLogByFieldsErrorExit(err,
					logging.ERROR, ehand.ERR_INVALID_OPTION)
			} else if len(this.redisAddrs) == 0 {
				g_loger.WriteToLogByFieldsExitMsgNoErr(fmt.Sprintf("no valid redis addr found from file %s", this.addrFile),
					logging.ERROR, ehand.ERR_ERROR)
			}
		}

	} else {
		//this.ifAddrFile = false
		if this.host == "" || int(this.port) == 0 {
			g_loger.WriteToLogByFieldsExitMsgNoErr("no redis addr is set",
				logging.ERROR, ehand.ERR_INVALID_OPTION)
		}
		this.redisAddrs = []myredis.RedisAddr{myredis.RedisAddr{Host: this.host, Port: int(this.port)}}
	}

	if this.keyfileDir != "" {
		this.ifKeyFiles = false
		if !filepath.IsAbs(this.keyfileDir) {
			this.keyfileDir = filepath.Join(gCwd, this.keyfileDir)
		}
		if this.dryRun {
			g_loger.WriteToLogByFieldsExitMsgNoErr(fmt.Sprintf("-dr is set to true to dry run to get target keys, but -kd is already set to %s which contain files with target keys",
				this.keyfileDir), logging.ERROR, ehand.ERR_ERROR)
		}
		this.keyFiles = map[string]string{}
		for _, addr := range this.redisAddrs {
			kFile, err := GetKeyFile(this.keyfileDir, addr)
			if err != nil {
				g_loger.WriteToLogByFieldsErrorExtramsgExit(err, fmt.Sprintf("%s has no file contains redis keys", addr.AddrString()),
					logging.ERROR, ehand.ERR_FILE_NOT_EXISTS)
			}
			this.keyFiles[addr.AddrString()] = kFile
			this.ifKeyFiles = true
		}

	} else {
		this.ifKeyFiles = false
	}

	/*

		if !mynumb.UintSliceContain(g_validRedisTypes, this.redisType) {
			g_loger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("-rt=%d is invalid, set it to %d. valid options are: %s",
				this.redisType, g_default_uints["redisType"],
				strings.Join(mystr.UintSliceToStringSlice(g_validRedisTypes), ",")), logging.ERROR)
			this.redisType = g_default_uints["redisType"]
		}
	*/

	if this.outdir != "" {
		if !kitsFile.IsExist(this.outdir) {
			g_loger.WriteToLogByFieldsErrorExit(fmt.Errorf("-o %s not exists", this.outdir),
				logging.ERROR, ehand.ERR_FILE_NOT_EXISTS)
		}
	} else {
		this.outdir = gCwd
	}

	if this.sampleRate > 100 {
		g_loger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("-sr=%d is out of range 0~100, set it to default 10",
			g_default_uints["sampleRate"]), logging.ERROR)
		this.sampleRate = g_default_uints["sampleRate"]
	} else if this.sampleRate == 0 {
		this.sampleRate = 100 // 0 is the same as 100
	}

	if this.limit != g_default_uints["limit"] && !this.ifSortedKey {
		g_loger.WriteToLogByFieldsErrorExit(fmt.Errorf("-li must work with -st"),
			logging.ERROR, ehand.ERR_OPTION_MISMATCH)
	}
	if this.ifSortedKey && this.commands == gc_command_bigkey {
		if this.bigKeyOnlyKeyName {
			this.ifSortedKey = false
			g_loger.WriteToLogByFieldsNormalOnlyMsg("-bn is specifed true, disable -st", logging.WARNING)
		} else {
			if this.limit == 0 {
				g_loger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("-li must > 0 when -st, set it to default %d",
					g_default_uints["limit"]), logging.WARNING)
				this.limit = g_default_uints["limit"]
			} else if this.limit > 1000 {
				g_loger.WriteToLogByFieldsNormalOnlyMsg("-li must < 1000 when -st, set it to 1000", logging.WARNING)
				this.limit = 1000
			} else {
				g_loger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("when -st and -li %d, will output biggest %d keys",
					this.limit, this.limit), logging.WARNING)
			}
		}

	}

	if this.elementScanBatch > this.elementBatch {
		g_loger.WriteToLogByFieldsExitMsgNoErr("-es must less than -ec", logging.ERROR, ehand.ERR_OPTION_MISMATCH)
	}

	if this.elementScanBatch > this.keyBatch {
		g_loger.WriteToLogByFieldsExitMsgNoErr("-es must less than -kc", logging.ERROR, ehand.ERR_OPTION_MISMATCH)
	}

	if this.minTtlSeconds < 0 && this.minTtlSeconds != -1 {
		g_loger.WriteToLogByFieldsExitMsgNoErr("-mint should -1 or integer >= 0", logging.ERROR, ehand.ERR_INVALID_OPTION)
	}
	if this.maxTtlSeconds < 0 && this.maxTtlSeconds != -1 {
		g_loger.WriteToLogByFieldsExitMsgNoErr("-maxt should -1 or integer >= 0", logging.ERROR, ehand.ERR_INVALID_OPTION)
	}

}

func (this *CmdConf) printHelpMsg() {
	fmt.Printf("\nUsage ./redis_scanner options\n\t%s\n\t%s\n\n", gc_version, gc_usage_comment)
	flag.PrintDefaults()
}

func GetResultFile(ifOnlyKeyName bool, cmd string, dir string, addr myredis.RedisAddr) string {
	str := ""
	switch cmd {
	case gc_command_bigkey:
		if ifOnlyKeyName {
			str = filepath.Join(dir, fmt.Sprintf("target_keys_%s_%d.txt", addr.Host, addr.Port))
		} else {
			str = filepath.Join(dir, fmt.Sprintf("bigkeys_%s_%d.csv", addr.Host, addr.Port))
		}

	case gc_command_dump:
		str = filepath.Join(dir, fmt.Sprintf("dumpkeys_%s_%d.txt", addr.Host, addr.Port))
	case gc_command_delete:
		str = filepath.Join(dir, fmt.Sprintf("deletedkeys_%s_%d.txt", addr.Host, addr.Port))
	default:
		str = filepath.Join(dir, fmt.Sprintf("%s_%s_%d.txt", cmd, addr.Host, addr.Port))
	}
	return str

}

func GetKeyFile(dir string, addr myredis.RedisAddr) (string, error) {
	keyFile := filepath.Join(dir, fmt.Sprintf("target_keys_%s_%d.txt", addr.Host, addr.Port))
	if kitsFile.IsFile(keyFile) {
		return keyFile, nil
	} else {
		return "", fmt.Errorf("%s not exists", keyFile)
	}
}

func GetTargetKeysFile(dir string, addr myredis.RedisAddr) string {
	return filepath.Join(dir, fmt.Sprintf("target_keys_%s_%d.txt", addr.Host, addr.Port))
}

// from ip:port to ip_port
func AddrStringToFileString(addr string) string {
	return strings.Replace(addr, ":", "_", 1)
}

func GetTargetKeysFileFromAddrString(dir string, addr string) string {
	return filepath.Join(dir, fmt.Sprintf("target_keys_%s.txt", AddrStringToFileString(addr)))
}

func GetChanBuf(cfg *CmdConf) int {
	chanBuf := MaxInt(int(2*cfg.pipeLineSize), int(2*cfg.threadsEachInstance))
	if chanBuf > 64 {
		chanBuf = 64
	}
	return chanBuf
}

func MaxInt(i int, j int) int {
	if i < j {
		return j
	} else {
		return i
	}
}

func MinInt(i int, j int) int {
	if i < j {
		return i
	} else {
		return j
	}
}
