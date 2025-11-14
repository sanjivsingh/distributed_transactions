import hashlib
import bisect
from xmlrpc import server


class Utils:

    @staticmethod
    def hash_key(key):
        """Generate hash for a key using MD5."""
        return int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16) % 121 


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
        print("added key:", key, " to vnode_hash:", str(Utils.hash_key(key)))
        self.data[vnode_hash][key] = value+":"+ str(Utils.hash_key(key))

    def delete(self, vnode_hash, key):
        if key in self.data[vnode_hash]:
            del self.data[vnode_hash][key]


class ConsistentHashing:
    def __init__(self, num_replicas=3):
        self.num_replicas = num_replicas  # Number of virtual nodes per server
        self.ring = []  # Sorted list of (hash, server) tuples
        self.server_hashes = {}  # server -> list of hashes
        self.servers = {}  # server -> list of hashes

    def add_server(self, server):
        """Add a server to the hash ring and identify keys that need to move."""
        if server in self.server_hashes:
            return

        self.server_hashes[server] = []
        change_ownership = []
        # number of virtual nodes per server.
        # each virtual node will be added to the ring
        ring_exten = []
        server_node = Server(server)
        for i in range(self.num_replicas):
            vName_name = f"{server}:{i}"
            hash_val = Utils.hash_key(vName_name)
            vName_name = f"{vName_name}:{hash_val}"
            self.server_hashes[server].append(hash_val)
            self.servers[server] = server_node
            server_node.add_vnode(hash_val)

            if len(self.ring) > 0:
                vnode, index = self.get_server_by_hash(hash_val)
                left_data_vnode = self.ring[index]
            else:
                left_data_vnode = None

            change_ownership.append((left_data_vnode, hash_val, server_node,vName_name))
            ring_exten.append((hash_val, server, vName_name))

        for ring_extend_item in ring_exten:
            print(" in ring : ", ring_extend_item)
            bisect.insort(self.ring, ring_extend_item)
        print(f"Added server: {server}")

        # Move   keys that now belong to the new server
        if change_ownership:
            for change_ownership_item in change_ownership:
                data_vnode, hash_val, server_node, vName_name = change_ownership_item
                if data_vnode is None:
                    continue

                data_server = data_vnode[1]
                data_server_start_hash = data_vnode[0]
                data_server_vnode = data_vnode[2]
                data_server_node = self.servers[data_server]


                right_index = (index + 1) % len(self.ring)
                right_vnode = self.ring[right_index]
                right_vnode_hash = right_vnode[0]

                keyes_moved = []
                for key in data_server_node.data[data_server_start_hash].keys():
                    key_hash = Utils.hash_key(key)
                    if  (hash_val <= key_hash < right_vnode_hash):
                        keyes_moved.append(key)
                    if (data_server_start_hash  > right_vnode_hash and (hash_val <= key_hash and key_hash < right_vnode_hash)) :
                        keyes_moved.append(key)

                if keyes_moved:
                    for key in keyes_moved:
                        value = data_server_node.get(data_server_start_hash,key)
                        data_server_node.delete(data_server_start_hash,key)
                        server_node.add(hash_val, key, value)
                        print( f"  Moved key '{key}' from {data_server_node.identifier} vname : {data_server_vnode} to {server_node.identifier}: vname : {vName_name}")

        else:
            print("No keys need to move.")

    def remove_server(self, server):
        """Remove a server from the hash ring and identify keys that need to move."""
        if server not in self.server_hashes:
            return

        for hash_val in self.server_hashes[server]:
            current_vnode , index  = self.get_server_by_hash(hash_val)
            light_index = (index -1) % len(self.ring)
            while self.ring[light_index] in self.server_hashes[server]:
                if light_index == 0:
                    light_index = len(self.ring) -1
                else:
                    light_index = (light_index - 1) % len(self.ring)

            keys_to_move = []
            next_vnode = self.ring[light_index]
            next_server = next_vnode[1]
            removed_server_node = self.servers[server]
            newowner_server_node = self.servers[next_server]

            for key_hash in removed_server_node.data[current_vnode[0]].keys():
                keys_to_move.append(key_hash)

            for key in keys_to_move:
                value = removed_server_node.get(current_vnode[0], key)
                newowner_server_node.add(next_vnode[0], key, value)
                removed_server_node.delete(current_vnode[0], key)
                print( f"  Moved key '{key}' from {current_vnode} to {next_vnode}")

        # Move keys to the next server in the ring
        for hash_val in self.server_hashes[server]:
            # Find and remove from ring
            for i, (hash_val, server_name, vName_name) in enumerate(self.ring):
                if hash_val == hash_val and server_name == server:
                    del self.ring[i]
                    break

        print(f"Removed server: {server}")
        del self.server_hashes[server]
        del self.servers[server]


    def get_server(self, key):
        """Get the server responsible for the key."""
        if not self.ring:
            return None
        hash_val = Utils.hash_key(key)
        print(f"Key '{key}' has hash '{hash_val}'")
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
            elif self.ring[index][0] <= hash_val < self.ring[index +1][0]  :
                idx = index
                return self.ring[idx], idx

        return self.ring[0], 0

    def add(self, key, value):
        """Add a key to track for migration."""
        print(key, " with hash ", str(Utils.hash_key(key)))
        vNode, index = self.get_server(key)
        next_index = (index + 1) % len(self.ring)
        next_node =  self.ring[next_index]
        print(f"Key '{key}' -> added -> Server '{vNode}'.  and Next Server '{next_node}'")
        self.servers[vNode[1]].add(vNode[0], key, value)

    def get(self, key):
        """Get a key's value from the appropriate server."""
        vNode, index = self.get_server(key)
        print(f"Key '{key}' -> retrieved from Server '{vNode[1]}' -> vNode '{vNode[2] }'")
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

