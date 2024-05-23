package org.apache.hadoop.ozone.container.merkletree;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerMerkleTree;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hadoop.ozone.container.merkletree.ContainerMerkleTreePersistence.readMerkleTreeFromDisk;
import static org.apache.hadoop.ozone.container.merkletree.ContainerMerkleTreePersistence.writeMerkleTreeToDisk;

/**
 * This class manages the Merkle Tree for a container.
 * Datanode can call the manager
 * 1. To update a Merkle Tree for a container.
 * 2. To read a Merkle Tree for a container.
 * 3. To delete a Merkle Tree for a container.
 * When updating the MerkleTree, the manager will serialize all updates and maintain a linear time line
 * of updates.
 * ToDo:
 * 1. Pass in the information needed to calculate the path of the container folder.
 * 2. Implement read and write from disk
 * 3. Add support for deletion hash
 */
public class ContainerMerkleTreeManager {
    ConcurrentHashMap<Long, Object> containerConcurrentMap = new ConcurrentHashMap<>();
    ConfigurationSource conf;
    ContainerMerkleTreeMetrics metrics = new ContainerMerkleTreeMetrics();
    public ContainerMerkleTreeManager(ConfigurationSource conf) {
        this.conf = conf;

    }

    public void deleteBlocks (ContainerData containerData, List<BlockID> blockIDS) throws IOException {
        Object lock = containerConcurrentMap.computeIfAbsent(containerData.getContainerID(), k -> new Object());
        synchronized (lock) {
            // 1. Read the container merkle tree from disk
            ContainerMerkleTree containerMerkleTree = readMerkleTreeFromDisk(containerData, metrics);
            // 2. Delete the blocks.
            ContainerMerkleTree.Builder containerMerkleTreeBuilder = ContainerMerkleTree.newBuilder(containerMerkleTree);
            for (BlockID blockID : blockIDS) {deleteBlock(containerMerkleTreeBuilder, blockID);
            }
            // 3. Update container Merkle Tree checksum
            containerMerkleTree = updateContainerMerkleTreeChecksum(containerMerkleTreeBuilder);
            // 4. Write the container Merkle Tree to disk.
            writeMerkleTreeToDisk(containerData, containerMerkleTree, metrics);
        }
    }

    private ContainerMerkleTree updateContainerMerkleTreeChecksum(ContainerMerkleTree.Builder containerMerkleTreeBuilder) {
        // Update the checksum of the container Merkle Tree.
        return null;
    }

    private void deleteBlock(ContainerMerkleTree.Builder containerMerkleTreeBuilder, BlockID blockID) {
        // Delete the block from the Merkle Tree.
        for (int i = 0; i < containerMerkleTreeBuilder.getBlockMerkleTreeList().size(); i++) {
            if (containerMerkleTreeBuilder.getBlockMerkleTree(i).getBlockID().getLocalID() == blockID.getLocalID()) {
                // mark the block deleted
                break;
            }
        }
    }

    public void updateBlocks(ContainerData containerData, List<BlockMerkleTreeManager> blockMerkleTreeManagerList) throws IOException {
        // 1. Read the container merkle tree from disk
        // 2. Update the blocks.
        // 3. Update container Merkle Tree checksum
        // 4. Write the container Merkle Tree to disk.
        Object lock = containerConcurrentMap.computeIfAbsent(containerData.getContainerID(), k -> new Object());
        synchronized (lock) {
            ContainerMerkleTree containerMerkleTree = readMerkleTreeFromDisk(containerData, metrics);
        }
    }
}
