/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"


//#define DEBUG
namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTree Node Allocation and debug Auxiliary Functions
// -----------------------------------------------------------------------------
	
NonLeafNodeInt *BTreeIndex::allocNonLeaf(PageId &pageId)
{
	NonLeafNodeInt *node;
	bufMgr->allocPage(file, pageId, (Page *&)node);
	memset(node, 0, Page::SIZE);
	return node;
}

LeafNodeInt *BTreeIndex::allocLeaf(PageId &pageId) 
{
	LeafNodeInt *node;
	bufMgr->allocPage(file, pageId, (Page *&)node);
	memset(node, 0, Page::SIZE);
	node->rightSibPageNo = 0;
	node->level = -1;
	return node;
}

void BTreeIndex::printNode(PageId pid)
{
	Page *page;
    bufMgr->readPage(file, pid, page);
	//print leaf page
	if(isLeaf(page))
	{
		LeafNodeInt *leaf = (LeafNodeInt *)page;
		for(int i = 0; i < INTARRAYLEAFSIZE; i++)
		{
			if(leaf->ridArray[i].page_number != 0){
				printf("page: %u, idx: %d, key: %d\n", pid, i, leaf->keyArray[i]);
			}
			else
			{
				break;
			}
		}
	}
	//print nonLeaf page
	else
	{
		NonLeafNodeInt *nonLeaf = (NonLeafNodeInt *)page;
		for(int i = 0; i < INTARRAYNONLEAFSIZE; i++)
		{
			if(nonLeaf->pageNoArray[i+1] != 0){
				printf("page: %u, idx: %d, key: %d\n", pid, i, nonLeaf->keyArray[i]);
			}
			else
			{
				break;
			}
		}
	}
	bufMgr->unPinPage(file, pid, false);
}

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
	std :: ostringstream idxStr ;
	idxStr << relationName << '.' << attrByteOffset ;
	outIndexName = idxStr.str () ; // indexName is the name of the index file
	bufMgr = bufMgrIn;
	scanExecuting = false;
	try
	{
		// try to open the index file
		file = new BlobFile(outIndexName, false); // throw FileNotFoundException if the file does not exist
		// if success, get and set the header page and root page
		headerPageNum = file->getFirstPageNo();
		Page *headerPage = NULL;
		bufMgr->readPage(file, headerPageNum, headerPage);
		IndexMetaInfo *meta = (IndexMetaInfo *)headerPage; // cast the first page to the meta page, then reference the meta data here
		strncpy((char *)(&(meta->relationName)), relationName.c_str(), 20);
		meta->relationName[19] = 0;
		meta->attrByteOffset = attrByteOffset;
		meta->attrType = attrType; 
		rootPageNum = meta->rootPageNo;

		// unpin the header page
		bufMgr->unPinPage(file, headerPageNum, false);
	}
	// if the index file does not exist. then catch the FileNotFoundException
	catch (FileNotFoundException e)
	{
		// create a new blob file for the index file
		file = new BlobFile(outIndexName, true);
		// allocate root and header page
		Page *headerPage = NULL;
		bufMgr->allocPage(file, headerPageNum, headerPage);	
		allocLeaf(rootPageNum);
		// fill meta info
		IndexMetaInfo *meta = (IndexMetaInfo *)headerPage; // cast the first page to the meta page, then reference the meta data here
		meta->attrByteOffset = attrByteOffset;
		meta->attrType = attrType;
		meta->rootPageNo = rootPageNum;
		strncpy((char *)(&(meta->relationName)), relationName.c_str(), 20);
		meta->relationName[19] = 0;

		bufMgr->unPinPage(file, headerPageNum, true);
		bufMgr->unPinPage(file, rootPageNum, true);
		// insert the records into the b+ tree (index file)
		FileScan fileScan(relationName, bufMgr);
		try
		{
			RecordId outRid;
			while (1)
			{
				fileScan.scanNext(outRid);
				std::string record = fileScan.getRecord();
				int key = *((int *)(record.c_str() + attrByteOffset));
				//printf("key: %d\n", key);
				insertEntry(&key, outRid); // insert the <key, rid> pair
			}
		}
		catch (EndOfFileException e)
		{
			// save B+ tree file to disk
			bufMgr->flushFile(file);
		}
	}
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
	scanExecuting = false;
  	bufMgr->flushFile(BTreeIndex::file);
  	delete file;
  	file = nullptr;
  	bufMgr = nullptr;
  	nextEntry = -1;
  	headerPageNum = 0;
  	rootPageNum = 0;
  	currentPageNum = 0;
  	currentPageData = nullptr;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

bool BTreeIndex::insertNode(int key, RecordId rid, int& midKey, PageId pid, PageId& newSonPageId)
{
	Page *curPage;
	bool split = false;
	bufMgr->readPage(file, pid, curPage);
	NonLeafNodeInt* nonLeaf = (NonLeafNodeInt *)curPage;

	//current node is leaf node
	if(nonLeaf->level == -1) 	
	{
		bufMgr->unPinPage(file, pid, false);
		//insert new entry to leaf, split is set to true if the node is splitted
		split = insertToLeaf(key, rid, pid, midKey, newSonPageId); 
	}
	else //non-leaf node
	{
		//find the index of key
		int idx = findNonLeafIndex(nonLeaf, key);
		PageId sonPid = nonLeaf->pageNoArray[idx];
		bufMgr->unPinPage(file, pid, true);
		//recursively find the leaf node, insert a pushed up entry to current node if son is splitted
		if(insertNode(key, rid, midKey, sonPid, newSonPageId))
		{
			int sonMidKey = midKey;
			PageId sonPageId = newSonPageId;
			split = insertToNonLeafNode(sonMidKey, sonPageId, pid, midKey, newSonPageId);
		}	
	}
	return split;
}


bool BTreeIndex::insertToNonLeafNode(int key, PageId sonPid, PageId pid, int& midKey, PageId& newPid)
{
	Page *curPage;
	bufMgr->readPage(file, pid, curPage);
	NonLeafNodeInt* node = (NonLeafNodeInt *)curPage;

	//printf("insert to non-leaf, pid:%u\n", pid);
	bool split = false;
	//if node is full, split page
	if(node->pageNoArray[INTARRAYNONLEAFSIZE] != 0) 
	{
		split = true;
		bufMgr->unPinPage(file, pid, true);
		midKey = splitNonLeafNode(key, sonPid, pid, newPid);	
	}
	else //insert a new key and a pointer to the son to the right of the key
	{
		int i = 0;
		//find the index where the new key should be inserted to
		while(i < INTARRAYNONLEAFSIZE && node->pageNoArray[i+1] != 0 && node->keyArray[i] <= key)
		{
			i++;
		}
		int j = INTARRAYNONLEAFSIZE - 1;
		//move all succeeding entries backward
		while(j > i)
		{
			node->keyArray[j] = node->keyArray[j-1];
			node->pageNoArray[j+1] = node->pageNoArray[j];
			j--;
		}
		//insert new entry
		node->keyArray[i] = key;
		node->pageNoArray[i+1] = sonPid;	
		bufMgr->unPinPage(file, pid, true);
	}
	return split;
}



bool BTreeIndex::insertToLeaf(int key, RecordId rid, PageId pid, int& midKey, PageId& newPid)
{
	Page *curPage;
	bufMgr->readPage(file, pid, curPage);
	LeafNodeInt* node = (LeafNodeInt *)curPage;
	
	bool split = false;
	int next = 0;

	//test whether node is full
	while(node->ridArray[next].page_number != 0 && next < INTARRAYLEAFSIZE)
	{
		next++;
	}
	//insert and split if node is full
	if(next == INTARRAYLEAFSIZE)
	{ 
		bufMgr->unPinPage(file, pid, true);
		midKey = splitLeafNode(key, rid, pid, newPid);
		split = true;
	}
	else //insert an entry if node is not full
	{
		int i = 0;
		if(node->ridArray[0].page_number == 0) //if the node is empty
		{
			node->keyArray[0] = key;
			node->ridArray[0] = rid;
			bufMgr->unPinPage(file, pid, true);
			return split;
		}

		//find the index for the new key
		while(i < INTARRAYLEAFSIZE)
		{
			if(node->ridArray[i].page_number == 0 || node->keyArray[i] > key)
			{
				int j = INTARRAYLEAFSIZE - 1;
				//move all succeeding entries backward
				while(j > i)
				{
					node->keyArray[j] = node->keyArray[j-1];
					node->ridArray[j] = node->ridArray[j-1];
					j--;
				}
				node->keyArray[i] = key;
				node->ridArray[i] = rid;
				break;
			}
			i++;
		}
		bufMgr->unPinPage(file, pid, true);
	}
	return split;
} 


// return midVal of the newly splitted node
int BTreeIndex::splitNonLeafNode(int key, PageId sonPid, PageId pid, PageId& newPid)
{
	Page *curPage;
	bufMgr->readPage(file, pid, curPage);
	NonLeafNodeInt* node = (NonLeafNodeInt *)curPage;
	int tempKey[INTARRAYNONLEAFSIZE+1], tempPid[INTARRAYNONLEAFSIZE+1];

	//create array with newly inserted entry
	tempPid[0] = node->pageNoArray[0];
	for(int i = 0, j = 0; i <= INTARRAYNONLEAFSIZE; i++, j++)
	{
		//insert new pair to the temp array
		if(j == INTARRAYNONLEAFSIZE || node->pageNoArray[j+1] == 0)
		{
			tempKey[j] = key;
			tempPid[j+1] = pid;
			break;
		}
		if(j == i && key < node->keyArray[i])
		{
			tempKey[i] = key;
			tempPid[i+1] = sonPid;
			i++;
		}
		tempKey[i] = node->keyArray[j];
		tempPid[i+1] = node->pageNoArray[j+1];
	}

	int midKey = tempKey[INTARRAYNONLEAFSIZE/2];

	//split two nodes
	NonLeafNodeInt *newNode = allocNonLeaf(newPid);
	newNode->level = node->level;
	for(int i = INTARRAYNONLEAFSIZE/2+1; i <= INTARRAYNONLEAFSIZE+1; i++)
	{
		node->pageNoArray[i] = 0; //mark unused array index
		newNode->pageNoArray[i-INTARRAYNONLEAFSIZE/2-1] = tempPid[i];
		if(i != INTARRAYNONLEAFSIZE+1)
		{
			newNode->keyArray[i-INTARRAYNONLEAFSIZE/2-1] = tempKey[i];
		}
	}
	bufMgr->unPinPage(file, newPid, true);
	bufMgr->unPinPage(file, pid, true);
	return midKey;
}


// split leaf and return mid value
int BTreeIndex::splitLeafNode(int key, RecordId rid, PageId pid, PageId& newPid)
{
	Page *curPage;
	bufMgr->readPage(file, pid, curPage);
	LeafNodeInt* node = (LeafNodeInt *)curPage;
	int tempKey[INTARRAYLEAFSIZE+1];
	RecordId tempRid[INTARRAYLEAFSIZE+1];
	
	//create array with newly inserted entry
	for(int i = 0, j = 0; i <= INTARRAYLEAFSIZE; i++, j++) 
	{
		//insert new pair to the temp array
		if(j == INTARRAYLEAFSIZE || node->ridArray[j].page_number == 0)
		{
			tempKey[j] = key;
			tempRid[j] = rid;
			break;
		}
		if(j == i && key < node->keyArray[i]) 
		{
			tempKey[i] = key;
			tempRid[i] = rid;
			i++;
		}
		tempKey[i] = node->keyArray[j];
		tempRid[i] = node->ridArray[j];
	}

	//split two nodes
	LeafNodeInt *newNode = allocLeaf(newPid);
	for(int i = INTARRAYLEAFSIZE/2; i <= INTARRAYLEAFSIZE; i++)	
	{
		if(i < INTARRAYLEAFSIZE)
			node->ridArray[i].page_number = 0; //mark unused array index
		newNode->ridArray[i-INTARRAYLEAFSIZE/2] = tempRid[i];
		newNode->keyArray[i-INTARRAYLEAFSIZE/2] = tempKey[i];
	}

	//update leaf node linked list
	newNode->rightSibPageNo = node->rightSibPageNo;
	node->rightSibPageNo = newPid;
	
	bufMgr->unPinPage(file, pid, true);
	bufMgr->unPinPage(file, newPid, true);
	return tempKey[INTARRAYLEAFSIZE/2];// return mid key;
}

void BTreeIndex::updateRootPageNo()
{
	Page *headerPage;
	bufMgr->readPage(file, headerPageNum, headerPage);
	IndexMetaInfo *header = (IndexMetaInfo *)headerPage;
	header->rootPageNo = rootPageNum; 
	bufMgr->unPinPage(file, headerPageNum, true);
}

//
const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
	Page *curPage;
	PageId pid = rootPageNum;
	bufMgr->readPage(file, pid, curPage);
	if(((LeafNodeInt*)curPage)->level == -1) //if root is leafnode
	{
		//printf("root is leaf, pid:%u\n", pid);
		bufMgr->unPinPage(file, pid, true);
		int midKey;
		PageId newPid;
		if(insertToLeaf(*(int *)key, rid, pid, midKey, newPid)) //need to split root
		{
			//allocate new root node and assign the two son node entries
			NonLeafNodeInt *newRoot = allocNonLeaf(rootPageNum);
			newRoot->level = 1;
			newRoot->keyArray[0] = midKey;
			newRoot->pageNoArray[0] = pid;
			newRoot->pageNoArray[1] = newPid;
			bufMgr->unPinPage(file, rootPageNum, true);
			//update root page number in header page
			updateRootPageNo();
		}
	} 
	else
	{
		bufMgr->unPinPage(file, pid, true);
		int midKey;
		PageId newPid;
		if(insertNode(*(int *)key, rid, midKey, rootPageNum, newPid)) // need to split root
		{
			//allocate new root node and assign the two son node entries
			NonLeafNodeInt *newRoot = allocNonLeaf(rootPageNum);
			newRoot->level = 0;
			newRoot->keyArray[0] = midKey;
			newRoot->pageNoArray[0] = pid;
			newRoot->pageNoArray[1] = newPid;
			bufMgr->unPinPage(file, rootPageNum, true);
			//update root page number in header page
			updateRootPageNo();
		}
	}
}



// ------------------------------------
// scan
// ----------------------------------- 
bool BTreeIndex::isLeaf(Page *page)
{
    return ((LeafNodeInt *)page)->level == -1;
}

void BTreeIndex::recursiveScanPageId()
{
    bufMgr->readPage(file, currentPageNum, currentPageData);
    if(isLeaf(currentPageData)) {
    	bufMgr->unPinPage(file, currentPageNum, false);
		return;
	}
    
    //else the page is an internal node
    NonLeafNodeInt *internal = (NonLeafNodeInt *) currentPageData;
    int idx = findNonLeafIndex(internal, lowValInt);
	PageId nextPage = internal->pageNoArray[idx];
    bufMgr->unPinPage(file, currentPageNum, false);
	currentPageNum = nextPage;
    recursiveScanPageId();
}

int BTreeIndex::findNonLeafIndex(NonLeafNodeInt *node, int key)
{
    int idx = 0;
	while(node->pageNoArray[idx+1] != 0 && key > node->keyArray[idx] && idx < INTARRAYNONLEAFSIZE){idx++;}
	return idx;
}



// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
    lowValInt = *((int *) lowValParm);
    highValInt = *((int *) highValParm);
    lowOp = lowOpParm;
    highOp = highOpParm;
    
    if(lowOp != GT && lowOp != GTE) throw BadOpcodesException();
    if(highOp != LT && highOp != LTE) throw BadOpcodesException();
    if(lowValInt > highValInt) throw BadScanrangeException();
    
    //if another scan is already executing, end here
    if(scanExecuting) 
		endScan();
  
    currentPageNum = rootPageNum;
    recursiveScanPageId();
	bufMgr->readPage(file, currentPageNum, currentPageData);
    //now we try to find the smallest entry that satisfy the operator
    LeafNodeInt *leaf = (LeafNodeInt *) currentPageData;

	bool found_flag = false;
    for (int i = 0; i < INTARRAYLEAFSIZE; i++)
	{
		int key = leaf->keyArray[i];
		// search to the end of the current node
        if(leaf->ridArray[i].page_number == 0)
        {
			break; 
        }else
        {
            // make sure if the key is within the correct range, stop searching if entry is found
            if(lowOp == GT && key > lowValInt)
            {
                nextEntry = i;
				found_flag = true;
				break;
            }
                
            if(lowOp == GTE && key >= lowValInt)
            {
                nextEntry = i;
				found_flag = true;
				break;
            }
               
            if(highOp == LT && key > highValInt)
            {
        		bufMgr->unPinPage(file, currentPageNum, false);
                throw NoSuchKeyFoundException();
            }
                
            if(highOp == LTE && key >= highValInt)
            {
        		bufMgr->unPinPage(file, currentPageNum, false);
                throw NoSuchKeyFoundException();
            }
        }
    }
	
    if(found_flag == false)
	{
		//if the key is not in the current page and there is no next page exists, throw exception
		if(leaf->rightSibPageNo == 0)
        {
        	bufMgr->unPinPage(file, currentPageNum, false);
            throw NoSuchKeyFoundException();
        } else //otherwise, the first entry of the next page is our target
        {
			PageId nextPage = leaf->rightSibPageNo;
			bufMgr->unPinPage(file, currentPageNum, false);
            currentPageNum = nextPage;
    		bufMgr->readPage(file, currentPageNum, currentPageData);
            nextEntry = 0;
        }
	}    
    scanExecuting = true;
}



// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid)
{
    if (!scanExecuting)
    {
        throw ScanNotInitializedException();
    }

    // Cast page to node
    LeafNodeInt *currentNode = (LeafNodeInt *)currentPageData;

    outRid = currentNode->ridArray[nextEntry];

    int val = currentNode->keyArray[nextEntry];
    
	// Check if the key is in range
  	if (val > highValInt || (val == highValInt && highOp == LT))
    {
        bufMgr->unPinPage(file, currentPageNum, false);
        throw IndexScanCompletedException();
	}
    
	nextEntry++;

    // if the scanner reach to the end of this page
    if (nextEntry == INTARRAYLEAFSIZE || currentNode->ridArray[nextEntry].page_number == 0)
    {
        // No more next leaf
        if (currentNode->rightSibPageNo == 0)
        {
        	bufMgr->unPinPage(file, currentPageNum, false);
            throw IndexScanCompletedException();
        }
        // Unpin page and read next page
        PageId nextPageNum = currentNode->rightSibPageNo;
        bufMgr->unPinPage(file, currentPageNum, false);
		currentPageNum = nextPageNum;
        bufMgr->readPage(file, currentPageNum, currentPageData);
        currentNode = (LeafNodeInt *)currentPageData;
        // Reset nextEntry
        nextEntry = 0;
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{
	if (!scanExecuting)
  	{
    	throw ScanNotInitializedException();
  	}

  	// reset the values
  	scanExecuting = false;
  	nextEntry = -1;
  	currentPageData = nullptr;
  	currentPageNum = 0;
}

}
