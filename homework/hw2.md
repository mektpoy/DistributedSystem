# MapReduce作业
计55 许翰翔 2015011370

## 代码分析

请仔细阅读下面这段关于MapReduce的伪代码（使用Hadoop MapReduce）。这段代码最终将会输出某一个大型文档集合中的出现次数最多的三个单词。

```JAVA
	void Map(String DocID, Text content) {
		HashMap<word, count> wordmap=new HashMap(……);
		for each word in content{
			if word not in wordmap
				wordmap.put(word,1);
			else
				wordmap.get(word).count++;
		}
		Emit("donemap",wordmap);
	}
	void Reduce(String key, Iterator<HashMap> maps) {
		HashMap<word, count> allwords = new HashMap(……);
		List<word, count> wordlist = new List(……);
		for map in maps{
			for each (word, count) in map
				if word not in allwords
					allwords.put(word,count)
				else
					allwords.get(word)+=count;
		}
		for each (word, count) in allwords
			wordlist.add(word,count);
		sort(wordlist) on count;
		Emit(wordlist.get(0));
		Emit(wordlist.get(1));
		Emit(wordlist.get(2));
	}
```

小华同学运行上述的代码，发现部分reducers会出现OutOfMemoryException的错误。请结合代码分析其原因(不要指出语法错误)。
针对上述错误，你有什么修改方案？请简要说明你的修改方案。

问题在于这段代码map阶段将所有的wordmap都放在了同一个key("donemap")里面，于是在Reduce阶段获得了全部的wordmap，占用了同一台机器的大量的内存并且实际上这步并没有利用分布式系统的优点，且导致了OutOfMemoryException。

我认为的改进方案主要有下：

- 将上述过程改成WordCount的做法，Map函数每次Emit(word, "1")，Reduce函数统计出同一个word的总数。
- 再增加一个MapReduce，并且上述的WordCount的输出作为这次的输入，这次的任务是获取最大的三个Value，这一步可以使用选择排序算法。
- 具体而言，Map函数将获得到的k-v组合按某种分配方式均匀（当然要保证原来相同的key打包后也相同）打包成同一个key给Reducer，而Reducer使用选择排序算法找到出现频率最高的三个单词，将这三个单词用于更新全局的答案即可。

## 投机执行
在论文中提到了，为了能够提高系统执行的速度，会采用投机执行的办法（speculative）。投机执行为何能够提高系统执行的速度？投机执行是否总是有效的，如果是否的话举出投机执行失效的场景？

在论文中并没有提到投机执行这个词，但是讨论过类似的手段（3.6节Backup Tasks）。它有效的原因是，由于MapReduce设计之初考虑的环境是一堆廉价的机器。其中经常会出现硬件或软件的故障，所以要考虑系统的健壮性，不应受到机器性能问题的制约。投机执行考虑的就是这样的事情，我们会在负载宽裕的节点重复提交某些task，当同一个任务第一次被执行完成后就杀死其他正在执行该任务的进程。这样，如果出现某个节点由于故障，例如磁盘故障导致读写速度缓慢，则我们获得了一个正常节点出色的完成了任务，系统不会因此整体速度被拖累，于是提高了整个系统的速度。

投机执行并不总是有效的，这是因为该场景与机器故障密切相关，当整体的机器情况良好，没有哪台机子出现了严重问题时，系统不会受到影响，如果当所有的任务分配均衡，每台机器获得的任务完成时间都差不多，则投机执行导致了做出了一些冗余任务，反而性能下降。

## 节点失效
一个MapReduce程序运行在100个节点上。每个节点可以同时运行4个任务，或者是4个Map任务，或者是4个Reduce任务。假设程序中需要运行的工作是40,000个map任务以及5,000个reduce任务。假设有一个节点坏掉了，那么最多需要重启多少个map任务？最多需要重启多少个reduce任务？为什么。

Master节点挂掉的话，需要从上一次的备份开始运行，则此时需要重启的任务就是挂掉的那一刻与上次备份之间做过的任务。
如果是Worker节点挂了，则只需要重启这台worker上运行的任务即可，即最多4个map任务或者reduce任务。