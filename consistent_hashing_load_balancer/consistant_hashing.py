import hashlib
import bisect

class Utils:

    @staticmethod
    def hash_key(key):
        """Generate hash for a key using MD5."""
        return int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16)  % (2**64)   

class Server:
    def __init__(self, identifier):
        self.identifier = identifier
        self.hashs = []
        self.data = {}

    def add_vnode(self, vnode_hash):
        bisect.insort(self.hashs, vnode_hash)
        self.data[vnode_hash] = {}

    def delete_vnode(self, vnode_hash):
        self.hashs.remove(vnode_hash)
        del self.data[vnode_hash]

    def get(self, vnode_hash, key):
        if key not in self.data[vnode_hash]:
            return None
        return self.data[vnode_hash][key]

    def add(self, vnode_hash, key, value):
        self.data[vnode_hash][key] = value

    def delete(self, vnode_hash, key):
        if key in self.data[vnode_hash]:
            del self.data[vnode_hash][key]


class ConsistentHashing:
    def __init__(self, num_replicas=3):
        self.num_replicas = num_replicas  # Number of virtual nodes per server
        self.ring = []  # Sorted list of (hash, server) tuples
        self.server_hashes = {}  # server -> list of hashes
        self.servers = {}  # server -> list of hashes

    def _generate_unique_hash(self, serverName, vIndex,vName_name):
            hash_val = Utils.hash_key(vName_name)
            j=1
            while hash_val in [item[0] for item in self.ring]:
                vName_name = f"{serverName}:{vIndex}_{j}"
                hash_val = Utils.hash_key(vName_name)
                j+=1
            return hash_val

    def add_server(self, server_name):
        """Add a server to the hash ring and identify keys that need to move."""
        if server_name in self.server_hashes:
            return

        self.server_hashes[server_name] = []
        # number of virtual nodes per server.
        # each virtual node will be added to the ring
        ring_extens = []
        server_node = Server(server_name)
        for i in range(self.num_replicas):
            vName_name = f"{server_name}:{i}"
            hash_val = self._generate_unique_hash(server_name, i, vName_name)
            vName_name = f"{vName_name}:{hash_val}"
            self.server_hashes[server_name].append(hash_val)
            self.servers[server_name] = server_node
            server_node.add_vnode(hash_val)
            ring_extens.append((hash_val, server_name, vName_name))

        for ring_extend_item in ring_extens:
            bisect.insort(self.ring, ring_extend_item)

        if len(self.ring) == len(ring_extens):
            #print("First server added, no keys to move.")
            return

        # Move  keys that now belong to the new server
        new_vnode_hashs = [item[0] for item in ring_extens]
        if ring_extens:
            for ring_extend_item in ring_extens:
                hash_val, server_name, vName_name = ring_extend_item

                new_vnode  , current_index = self.get_server_by_hash(hash_val)
                prev_vnode  , prev_server_index = self.get_server_by_hash(hash_val-1)
                while self.ring[prev_server_index][0] in new_vnode_hashs:
                    if prev_server_index == 0:
                        prev_server_index = len(self.ring) - 1
                    else:
                        prev_server_index = (prev_server_index -1) 
    
                data_vnode = self.ring[prev_server_index]
                data_server = data_vnode[1]
                data_server_start_hash = data_vnode[0]
                data_server_node = self.servers[data_server]

                next_vnode_hash = self.ring[(current_index + 1) % len(self.ring)][0]
                # Identify keys to move
                keyes_moved = set()
                for key in data_server_node.data[data_server_start_hash].keys():
                    key_hash = Utils.hash_key(key)
                    if hash_val < next_vnode_hash:
                        if  (hash_val <= key_hash < next_vnode_hash):
                            keyes_moved.add(key)
                    else:
                        if (hash_val <= key_hash or key_hash < next_vnode_hash) :
                            keyes_moved.add(key)

                if keyes_moved:
                    for key in keyes_moved:
                        value = data_server_node.get(data_server_start_hash,key)
                        data_server_node.delete(data_server_start_hash,key)
                        server_node.add(hash_val, key, value)
                
    def remove_server(self, server_name):
        """Remove a server from the hash ring and identify keys that need to move."""
        if server_name not in self.server_hashes:
            return

        for v_hash_val in self.server_hashes[server_name]:
            # virtual node being removed
            old_vnode , old_index  = self.get_server_by_hash(v_hash_val)
            
            # Find the left next server in the ring that is not being removed
            new_index = (old_index -1) % len(self.ring)
            while self.ring[new_index][0] in self.server_hashes[server_name]:
                if new_index == 0:
                    new_index = len(self.ring) -1
                else:
                    new_index = (new_index - 1) % len(self.ring)
            # Move keys to the next server in the ring
            self.__move_all_keys(old_index, new_index)

        for v_hash_val in self.server_hashes[server_name]:
            # Find vNode and remove from ring
            for i, (hash_val, s_name, v_name) in enumerate(self.ring):
                if v_hash_val == hash_val and server_name == s_name:
                    del self.ring[i]
                    break
        #print(f"Removed server: {server}")
        del self.server_hashes[server_name]
        del self.servers[server_name]

    def __move_all_keys(self, old_index, new_index, keys_to_move = None):
        old_vnode = self.ring[old_index]
        next_vnode = self.ring[new_index]
        old_server_node = self.servers[old_vnode[1]]
        new_server_node = self.servers[next_vnode[1]]
        if not keys_to_move:
            keys_to_move = list(self.servers[old_vnode[1]].data[old_vnode[0]].keys())
        if keys_to_move:
            for key in keys_to_move:
                value = old_server_node.get(old_vnode[0], key)
                new_server_node.add(next_vnode[0], key, value)
                old_server_node.delete(old_vnode[0], key)
                #print( f"  Moved key '{key}' from {current_vnode} to {next_vnode}")

    def get_server(self, key):
        """Get the server responsible for the key."""
        if not self.ring:
            return None
        hash_val = Utils.hash_key(key)
        return self.get_server_by_hash(hash_val)


    def get_server_by_hash(self, hash_val):
        """Get the server responsible for the key."""
        if not self.ring:
            return None
        # Find the first server with hash >= key's hash
        for index in range(len(self.ring)):
            if index  == 0:
                if hash_val < self.ring[index][0]:
                    idx = len(self.ring) -1
                    return self.ring[idx], idx
            if index  == len(self.ring) -1:
                if self.ring[index][0] <= hash_val:
                    idx = index
                    return self.ring[idx], idx
            elif self.ring[index][0] <= hash_val < self.ring[index +1][0]:
                idx = index
                return self.ring[idx], idx
        return self.ring[0], 0

    def add(self, key, value):
        """Add a key to track for migration."""
        vNode, index = self.get_server(key)
        self.servers[vNode[1]].add(vNode[0], key, value)

    def get(self, key):
        """Get a key's value from the appropriate server."""
        vNode, index = self.get_server(key)
        return self.servers[vNode[1]].get(vNode[0], key)


# Example usage
if __name__ == "__main__":

    ch = ConsistentHashing()
    ch.add_server("server1")
    ch.add_server("server2")

    # Add some keys
    number_of_keys = 20
    for index in range(1, number_of_keys):
        ch.add(f"key{index}", f"value{index}")

    keys = [f"key{i}" for i in range(1, number_of_keys)]
    print("\n".join([str(item) for item in ch.ring]))
    for key in keys:
        print(ch.get(key))

    print("\nAdding server3:")
    ch.add_server("server3")

    print("\n".join([str(item) for item in ch.ring]))
    for key in keys:
        print(ch.get(key))

    print("\nRemoving server2:")
    ch.remove_server("server2")

    print("\n".join([str(item) for item in ch.ring]))
    for key in keys:
        print(ch.get(key))

