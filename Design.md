
## 实现方案

> GaussDB(DWS)、AnalyticDB、Singlestore、Unistore(snowflake)
在HTAP实时入库和查询场景下, 在行式实时入库到转换为列式存储中间添加一个行式增量存储, 用于支持实时查询. 
GaussDB(DWS)等主键模型HTAP数据库使用RocksDB作为行式存储(Delta Table)的存储引擎, 其特征呈现如下: 入库时, 一定时间内对于某Key范围进行Put(行式入库), 随后触发Merge, 顺序Scan然后Delete所有行; 此外, 同时也通过Primary Index支持实时Get查询. 
Delta Table是类似缓冲区的存在, 其数据量是有限的, 且每个Key最终都会被删除, 采用RocksDB的话会面临大量删除墓碑的GC问题, 带来大量不必要的数据下沉和Compaction开销. 为了优化: 
1. 仅留下L0层(语义是, 这些KV没有新旧之分, 因此不需要通过Level区分新旧), 且最终都会被删除, 不能下沉. 
2. 此时L0层会存在大量重叠的SST, 查询性能差, 因此对L0进行分区, 且需要是动态分区(目标是分区间大小均匀). 
3. 需要有墓碑GC机制, 在分区边界变化时进行Compaction同时进行GC.

### 目标负载

不同分布的读写混合, 每个Key最终都会被删除

key是row_id, key是随机读写混合, 在整个row_id范围内, 等大小划分区间, 区间内row_id达到一定程度就会作为转为列存, 并在此行存表中删除. 
参数如下: 
1. num 作为整个row_id空间 
2. delta_bench_rowset_num 区间数目, 每个区间的row达到一定比例之后就会转为列存(即执行范围删除)
3. delta_bench_delta_merge_count 指定时达到指定次数转列存操作(delta Merge, 范围删除)才停止

row_id作为Key, 随机读写; 将其分N个区, 每个分区内达到一定比例后执行Merge, 即删除该分区的Key. 
在db_bench中添加一个新的负载函数

### RocksDB相关功能

- FIFO Compaction
- Intra-L0 compaction
- SubCompaction
- Universal Compaction
- Flush/Compaction/VersionSet

### RocksDB 读取

- Get: 单线程对每个Key范围内的SST进行Get
- Scan: 
    - L0: 迭代每一个SST
    - L1+: 二分到指定SST

### RocksDB的结构

概念:
- Sorted Run: 指一段按 key 有序排列、且内部 key 不重复的数据集合
    即: L0 的每个SST、L1~Lmax的每一层
- RocksDB的Compaction: 发生在L0+的层之间, 不同Style下的Compaction没有本质区别, 都保证将输入来自多个层的多个输入文件进行归并排序后视情况不重叠地拆分输出到某个输出层中
- RocksDB Compaction Style: 
    > The key difference between the two strategies is that leveled compaction tends to aggressively merge a smaller sorted run into a larger one, while "tiered" waits for several sorted runs with similar size and merge them together.
    > If options.num_levels=1, we still follow the same placement rule. It means all the files will be placed under level 0 and each file is a sorted run. The behavior will be the same as initial universal compaction, so it can be used as a backward compatible mode.
    通过每个策略的CompactionPicker选择不同的SST生成Compaction供CompactionJob执行

### 具体实现

- 仅留下L0
    - L0无限大
- 动态分区
    - 每个分区内是Tiered.
    - 初始分区通过将第一个下刷的ImmTable确定(N/2个)
    - 何处分区: 修改FlushJob的Flush到L0时(WriteLevel0Table)
    - 何时更新分区: Flush/合并/拆分
    - 分区表具体实现:
        ```cpp
        using PartitionID = int64;
        struct PartitionTable { 
            struct PartitionInfo {
                int64 growth;
                PartitionID partition_id;
            }
            using PartitionStorage = std::map<Key, PartitionInfo>;
            PartitionStorage partition_map;
            std::multimap<int64, PartitionStorage::iterator> growth_rate_index;
            std::map<PartitionID, PartitionStorage::iterator> partition_id_index;
        };
        ```
- Flush
    > 需要解决小SST问题: 通过分区合并解决
    - Flush时根据分区表拆分SST
    - 更新分区表
- Compaction
    - 为什么?
        - 分区拆分
        - 分区合并(仅更新分区表, 不实际输出Compaction SST)
        - 小/大SST的产生: Key分布发生变化
    - 何时？(触发)
        - 分区增长率:
            - 合并: 数据增长率低于平均值一半的分区(存在小SST)
            - 拆分: 数据增长率超过平均值 1.5 倍的高写活动分区
    - 如何?
        - 合并: 读两个输入分区的所有SST
        - 拆分: 按照新的分区边界读入所有SST拆分
    - 修改
        - 需要实现新的CompactionPicker(NeedsCompaction, PickCompaction, ...), 决定是否触发Compaction以及如何Compaction
        - 需要修改VersionStorageInfo记录分区相关信息
    - 用途: 
        - 分区变化
        - 墓碑GC
- 墓碑清除(Bottommost Level)
    - Universal Compaction 包含最老的Sorted Run才清除墓碑
    - Leveled Compaction 通过层级确定新旧
- 分区表维护
    - Flush
        - 第一次: 构建分区表`[-INF, k1), [k1, k2), ..., [kn, +INF)`, 可以保证在分区表LogAndApply前不会有Compaction
        - 后续: 
            - 迭代每个分区, 收集每个分区的指定范围KV到一个SST
            - 更新分区表: edit->GetPartitionTableEditor()->UpdatePartitionKeyCount(partition_id, key_count)
    - Compaction
        - 通用
            - CompactionPicker选择指定分区内所有SST, 生成Compaction计划(可知分区信息)
            - CompactionJob实际运行时在VersionEdit中修改分区表并在Install时更新至Version
            - CompactionJob: 输出到指定分区(填目标分区ID)
        - Merge 
            - Picker: 通过`PartitionTable::GetMergePlan() 得到 MergePlan{left_pid, right_pid}`选择目标2分区(Compaction{inputs, max_subcompactions=1, plan, ...})
            - CompactionJob完成后调用`CompleteMerge(plan)`修改VersionEdit上的分区表
        - Split
            - Picker: 通过`PartitionTable::GetSplitPlan() 得到 SplitPlan{pid, new_pid}`选择目标分区
            - Split依赖max_subcompactions=2进行, Split的中间Key在GenSubcompactionBoundaries中得到
            - CompactionJob完成后调用`CompleteSplit(plan, mid_key)`修改VersionEdit上的分区表
- VersionEdit
    - Flush和Compaction可能并发执行, 通过VersionEdit将文件更改在LogAndApply中原子生效(类似commit)
    - Flush仅修改统计数据, 允许不准确

TODO:
- [x] 目标负载测试
- [x] 分区内小SST多，需要Compaction机制
- [x] 分区统计信息（growth/...）需要重新设计，并相应更新CompactionPicker机制

还需要怎样的Compaction机制
- 分区内使用仅L0下的Universal

存在的问题及拟采取的办法:
- [x] 目前Compaction尚未实现完全, 分区后每个分区内存在大量小文件, 计划通过在每个分区内执行原RocksDB的Universal Compaction进行合并
- [ ] 分区的合并拆分目前仅使用分区增长率衡量(单位时间内数据增量), 计划添加更加详细的分区内统计数据, 并根据后续实验效果调整分区的合并拆分的策略.

TODO, 关于Range Delete:
- [ ] L0 Range Delete Tombstone GC 
- [x] Range Tombstone Flush时并未裁剪到分区范围
- [ ] 当Range Delete覆盖整个分区时->参考FIFO Compaction, 执行Delete Compaction

关于CompactionPicker
- Compaction Score
- FilesMarkedForCompaction / IntTblPropCollector / CompactOnDeletionCollector
- Delete Compaction
- CompactRange

目前进度
- 已经完成分区内的UniversalCompactionBuilder
- 目前各分区是串行pick的, 即一次Picker仅生成一次Compaction对象

目标负载下, Split和Merge基本不会出现, 主要是分区内Universal Compaction, 添加`partition_file_num_compaction_trigger=4`

UniversalCompactionPicker在L0选取的都是连续的文件

TODO
- [ ] RangeDelete覆盖整个分区时, 直接Mark For Compaction/Delete, 并将此分区和相邻分区合并
- [ ] RangeDelete覆盖部分分区时
- [ ] 目前DeltaPicker是直接用了UniversalCompactionBuilder, 后面肯定是要复制一份DeltaBuilder然后在上面定制的

TODO
- [x] fix Leveled bug
- [ ] 正确性测试
- [ ] Increasing compaction threads because we have 17 level-0 files 