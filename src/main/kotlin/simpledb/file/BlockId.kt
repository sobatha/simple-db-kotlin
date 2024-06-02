package org.example.simpledb.file

/*
 * identifies a specific block by irs file name and logical block number
 */
data class BlockId(val fileName: String, val number: Int)