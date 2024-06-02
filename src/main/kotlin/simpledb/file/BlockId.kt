package org.example.simpledb.file

/*
 * identifies a specific block by irs file name and logical block number
 */
data class BlockId(private val fileName: String, private val number: Int)