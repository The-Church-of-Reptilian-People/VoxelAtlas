package fr.boul2gom.voxelatlas.dynmap.data;

import com.hypixel.hytale.server.core.universe.world.World;
import com.hypixel.hytale.server.core.universe.world.chunk.BlockChunk;
import com.hypixel.hytale.server.core.universe.world.storage.ChunkStore;
import com.hypixel.hytale.server.core.universe.world.storage.provider.IndexedStorageChunkStorageProvider.IndexedStorageCache;
import com.hypixel.hytale.server.core.universe.world.storage.provider.IndexedStorageChunkStorageProvider.IndexedStorageChunkLoader;
import com.hypixel.hytale.storage.IndexedStorageFile;
import com.hypixel.hytale.math.util.ChunkUtil;
import com.hypixel.hytale.component.Store;
import fr.boul2gom.voxelatlas.VoxelAtlas;

import java.util.concurrent.CompletableFuture;

/**
 * A helper record to access world chunks safely and asynchronously.
 * <p>
 * This class wraps a Hytale {@link World} instance and provides methods to
 * check for
 * chunk existence and retrieve blocks without triggering unwanted generation.
 * </p>
 * <p>
 * Constructor parameters:
 * <ul>
 * <li><b>world</b>: The Hytale world instance to access.</li>
 * </ul>
 * </p>
 */
public record ChunkAccessor(/** The Hytale world instance */ World world) {

    /**
     * Create a new chunk accessor for the given world.
     *
     * @param world The Hytale world instance to access.
     */
    public ChunkAccessor {
    }

    /**
     * Fetch a block chunk ONLY if it is already generated/loaded.
     * <p>
     * Use this method to avoid triggering chunk generation.
     * </p>
     *
     * @param chunkX The Chunk X coordinate.
     * @param chunkZ The Chunk Z coordinate.
     * @return A {@link CompletableFuture} containing the {@link BlockChunk} if
     *         available,
     *         or completing with null if the chunk is not generated or an error
     *         occurs.
     */
    public CompletableFuture<BlockChunk> get_block_chunk(int chunkX, int chunkZ) {
        // 1. Check unexplored status async first
        return is_unexplored(chunkX, chunkZ).thenCompose(unexplored -> {
            if (unexplored) {
                return CompletableFuture.completedFuture(null);
            }

            // 2. Load from disk safely using Hytale's async method
            try {
                final long index = ChunkUtil.indexChunk(chunkX, chunkZ);
                return world.getChunkStore().getChunkReferenceAsync(index, 4)
                        .thenApply(ref -> {
                            if (ref != null && ref.isValid()) {
                                final Store<ChunkStore> store = world.getChunkStore().getStore();
                                return store.getComponent(ref, BlockChunk.getComponentType());
                            }
                            return null;
                        });
            } catch (Exception e) {
                return CompletableFuture.completedFuture(null);
            }
        });
    }

    /**
     * Checks if a chunk is unexplored (not generated).
     *
     * @param chunkX The Chunk X coordinate.
     * @param chunkZ The Chunk Z coordinate.
     * @return A {@link CompletableFuture} containing true if the chunk is
     *         unexplored (not generated),
     *         false if it exists. Returns true if the world is null or on error.
     */
    public CompletableFuture<Boolean> is_unexplored(int chunkX, int chunkZ) {
        if (world == null) return CompletableFuture.completedFuture(true);

        return CompletableFuture.supplyAsync(() -> {
            try {
                final ChunkStore chunk_store = world.getChunkStore();
                final Store<ChunkStore> store = chunk_store.getStore();

                // This relies on the world using IndexedStorage
                /*if (chunk_store.getLoader() instanceof IndexedStorageChunkLoader) {
                    final var cache = store.getResource(IndexedStorageCache.getResourceType()); // getResourceType is removed in patch 3

                    int regionX = chunkX >> 5;
                    int regionZ = chunkZ >> 5;

                    // Checks if the region file exists
                    final IndexedStorageFile region_file = cache.getOrTryOpen(regionX, regionZ, false);
                    if (region_file != null) {
                        int localX = chunkX & 0x1F;
                        int localZ = chunkZ & 0x1F;
                        int index = ChunkUtil.indexColumn(localX, localZ);

                        // Check if the chunk index exists in the region file keys
                        if (region_file.keys().contains(index)) {
                            return false; // It exists!
                        }
                    }
                }*/
                if (chunk_store.getLoader() instanceof IndexedStorageChunkLoader) {
                    IndexedStorageCache cache = null;
                    try {
                        java.lang.reflect.Field cacheField = IndexedStorageChunkLoader.class.getDeclaredField("cache");
                        cacheField.setAccessible(true);
                        cache = (IndexedStorageCache) cacheField.get((IndexedStorageChunkLoader) chunk_store.getLoader());
                    } catch (Exception e) {
                        VoxelAtlas.LOGGER.atSevere().log("[VoxelAtlas] Failed to reflect cache: " + e.getMessage());
                        return true;
                    }

                    int regionX = chunkX >> 5;
                    int regionZ = chunkZ >> 5;

                    // Checks if the region file exists
                    final IndexedStorageFile region_file = cache.getOrTryOpen(regionX, regionZ, false);
                    if (region_file != null) {
                        int localX = chunkX & 0x1F;
                        int localZ = chunkZ & 0x1F;
                        int index = ChunkUtil.indexColumn(localX, localZ);

                        // Check if the chunk index exists in the region file keys
                        if (region_file.keys().contains(index)) {
                            return false; // It exists!
                        }
                    }
                }
            } catch (Exception e) {
                VoxelAtlas.LOGGER.atSevere().log("[VoxelAtlas] Error checking chunk generation: " + e.getMessage());
            }

            return true;
        });
    }
}
