package org.apache.hadoop.ozone.container.merkletree;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockMerkleTree;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.DatanodeBlockID;

public class BlockMerkleTreeManager {

    BlockMerkleTree.Builder merkleTree;
    public BlockMerkleTreeManager() {
        merkleTree = BlockMerkleTree.newBuilder();
    }
    public BlockMerkleTree.Builder getMerkleTree() {
        return merkleTree;
    }

    public DatanodeBlockID getBlockID() {
        return null;
    }
}
