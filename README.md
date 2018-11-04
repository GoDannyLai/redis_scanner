# 简介
    redis_scanner可以快速安全地分析出redis与redis cluster所有的big key，快速安全地删除匹配某个正则的所有KEY， 安全地dump出匹配某个正则的所有KEY的内容<br/>
	支持redis版本4.0及之后的memory usage 与unlink功能来分析KEY的大小与删除KEY
    redis_scanner解决了redis-cli --bigkeys抽样与list等复合类型的KEY不准确的问题， 也解决了现在某些同类型的工具速度慢的问题<br/>
        1）生成csv格式的bigkey报表， 指定-st -li XXX会按KEY大小排序并输出最大的XXX个KEY：
            redis_scanner -H 192.168.xx.125 -P 6380 -dc -o tmp -si 1024 -st -li 100 -ts -w bigkey -ps 1
![bigkey](https://github.com/GoDannyLai/trident/raw/master/misc/img/bigkey.png)
			结果文件每列的含义如下：
				db: redis database number
				sizeInByte: key所占内存的大小， 单位byte
				elementCnt: 复合类型key的元素个数
				type: key的类型
				expire: 过期时间， 空则为没有设置过期时间
				bigSize: 复合类型key所有元素中占用内存最大的元素的内存大小, 单位byte
				key: key名
				big: 最大元素名
![bigkey](https://github.com/GoDannyLai/trident/raw/master/misc/img/bigkeys_csv.png)

        2）dump出指定的KEY的value
        	redis_scanner -H 192.168.xx.125 -P 6380 -dc -o tmp -si 1024 -ts -w dump -ps 1
![bigkey](https://github.com/GoDannyLai/trident/raw/master/misc/img/dumpkey.png)
			结果为json格式， 加了-b更易读，否则是一行一个key
![bigkey](https://github.com/GoDannyLai/trident/raw/master/misc/img/dumpkey_json.png)			
           
        3) 删除指定的KEY
            安全起见，一般先dry run来输出所有符合条件的KEY， 然后再删除
![bigkey](https://github.com/GoDannyLai/trident/raw/master/misc/img/delete_dryrun.png)	
			真正删除dry run结果中的所有KEY
![bigkey](https://github.com/GoDannyLai/trident/raw/master/misc/img/delete.png)


# 参数解释
	-F 
		即使指定主库，默认也是从找到最新的从库来扫描key， 如果没有合适的从库来扫描，则报错。 -F则在没有合适的情况下， 使用主库来扫描
	-H
		redis ip地址
	-P 
		redis 端口
	-p 
		redis 密码
	-af
		redis地址文件。 一行一个地址， 地址格式为IP:port的形式， 可以指定多行多个地址。redis_scanner支持两个地址指定方式， 一种是-af， 另一种是-H与-P
	
	-kd
		从-kd指定的目录读入要处理的KEY列表。 文件名必须为target_keys_ip_port.txt， 一行一个KEY名。一般是-dr或者-bn输出的结果文件
	
	-n 
		redis database, 默认为0。
	-dc 
		对于redis cluster, -dc是自动发现集群的所有节点， 并扫描整个集群，否则只是扫描指定地址的节点
	-w 
		选择功能， 支持的功能为bigkey, dump, delete, 分别为扫描大key， dump出key内容与删除key
	-dr 
		dry run. 只是输出符合条件的key， 不作dump, delete或者扫描。 对于delete, 建议都先dry run再真正delete
	-ti 
		每个节点的处理线程， 默认为2。扫描线程与处理线程不同， 扫描线程固定为每个节点一个线程， 不可更改. 注意调整该参数， 以免redis qps过大,
		cpu过高
	-ps 
		pipeline大小， 与-ti是两个对速度与性能影响最大的参数。 支持redis与redis cluster, 不过如果扫描cluster 报across slots错误时
		-ps 只能指定为1了
	-es 
		扫描key名时，每次scan返回多少个KEY； 扫描一个复合类型的key时， 每次scan返回多少个元素
	-ec 
		扫描一个复合类型的key时,每处理-ec个元素就休眠-ei microseconds
	-ei 
		扫描一个复合类型的key时,每处理-ec个元素就休眠-ei microseconds
	-kc
		每处理-kc个key, 就休眠-ki microseconds
	-ki 
		每处理-kc个key, 就休眠-ki microseconds
	-k
		匹配key名正则表达式， 如果设置了， 只有匹配的key才会进入下一步处理。 注意命令行可能会转义的正则字符
	-si
		对于delete/dump， 只有占用内存超过-si指定的值的才会进入下一步处理. 单位为byte， 默认为3072
	-maxt 与 -mint
   		只处理过期时间默认条件的KEY, 单位second, 默认为0.
   		1)-mint==-1 and -maxt==-1: only keys not set expired time.
   		2)-mint==0 and -maxt==0: any key.
   		3)-mint > 0: only keys whose ttl >= -mint. 
   		4)-maxt > 0: only keys whose ttl <= -maxt

	-ts 
		默认如果自动检测到redis的版本大于等于4.0.0, 则使用memory usage来估算key的内存， 指定-ts， 则不管redis版本， 都是一个key一个key, 
		一个元素一个元素地读出来计算内存大小。
	-sr
		当使用memory usage来抽样检测key内存大小时， -sr指定抽样的百分比，仅作用于复合类型，即是抽样多少元素来统计KEY的内存
	-dd 
		默认是通过给key设置一个1到-ex之间随机的过期时间来删除KEY. 如果指定-dd， 则对于大于等于4.0.0版本的redis , 使用unlink来删除
		否则使用delete来删除
	-ex
		给KEY设置-ex秒过期时间
	-st
		-w=bigkey时， 结果按KEY的大小排序
	-li
		-w=bigkey时, 结果只输出最大的-li个key
	-bn
		-w=bigkey时, 结果只输出KEY的名字，一行一个key名
	-b 
		-w=dump时， 默认一行一个key的json格式内容， 指定-b则以更易读的形式输出json格式内容
	-o
		指定结果输出目录， 默认当前目录
	
# 联系
	有任何建议或者bug， 请联系laijunshou@gmail.com	   