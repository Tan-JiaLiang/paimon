/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.table;

/**
 * Bucket mode of the table, it affects the writing process and also affects the data skipping in
 * reading.
 *
 * 注意：
 * 1. bucket是读/写的最小存储单元，譬如bucket=1，则说明只能够单并行度进行读/写
 * 2. bucket过多，则导致小文件问题，bucket过少则导致写性能低下
 */
public enum BucketMode {

    /**
     * The fixed number of buckets configured by the user can only be modified through offline
     * commands. The data is distributed to the corresponding buckets according to bucket key
     * (default is primary key), and the reading end can perform data skipping based on the
     * filtering conditions of the bucket key.
     *
     * 固定bucket大小
     * 用户配置的bucket只能通过离线命令进行修改。
     * 数据根据bucket key（默认为主键）分配到对应的bucket中，读端可以基于bucket key的过滤条件进行数据跳过。
     * 该模式适用于AppendOnly表和Changelog表。
     */
    FIXED,

    /**
     * The Dynamic bucket mode records which bucket the key corresponds to through the index files.
     * This mode cannot support multiple concurrent writes or data skipping for reading filter
     * conditions. This mode only works for changelog table.
     *
     * bucket=-1，自动扩容bucket
     * 通过索引文件记录键对应的存储桶。该模式不支持多个并发写入，也不支持在读取过滤条件时跳过数据。
     * 该模式仅适用于Changelog表。
     */
    DYNAMIC,

    /**
     * Compared with the DYNAMIC mode, this mode not only dynamically allocates buckets for
     * Partition table, but also updates data across partitions. The primary key does not contain
     * partition fields.
     *
     * bucket=-1
     * 与DYNAMIC模式相比，该模式不仅能为分区表动态分配bucket，还能跨分区更新数据。主键不包含分区字段。
     * 该模式仅适用于Changelog表。
     */
    GLOBAL_DYNAMIC,

    /**
     * Ignoring buckets can be equivalent to understanding that all data enters the global bucket,
     * and data is randomly written to the table. The data in the bucket has no order relationship
     * at all. This mode only works for append-only table.
     *
     * bucket=-1
     * 忽略bucket就等于认为所有数据都进入了全局bucket、数据会随机写入表中。
     * 数据桶中的数据完全没有顺序关系。
     * 该模式仅适用于AppendOnly表。
     */
    UNAWARE
}
