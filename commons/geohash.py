import math

class GeohashConverter:
    """
    A utility to convert Latitude and Longitude into a Geohash string.
    Geohash is a public domain geocode system which encodes a geographic 
    location into a short string of letters and digits.
    """
    
    # Standard Base32 alphabet for Geohashing (excludes a, i, l, o)
    BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"
    
    @classmethod
    def encode(cls, latitude, longitude, precision=12):
        """
        Encodes coordinates into a geohash string.
        
        :param latitude: Float between -90 and 90
        :param longitude: Float between -180 and 180
        :param precision: Number of characters in the resulting hash (1 to 12)
        :return: Geohash string
        """
        lat_interval = [-90.0, 90.0]
        lon_interval = [-180.0, 180.0]
        
        geohash = []
        bits = 0
        ch = 0
        even_bit = True
        
        while len(geohash) < precision:
            if even_bit:
                # Longitude
                mid = (lon_interval[0] + lon_interval[1]) / 2
                if longitude > mid:
                    ch |= (1 << (4 - bits))
                    lon_interval[0] = mid
                else:
                    lon_interval[1] = mid
            else:
                # Latitude
                mid = (lat_interval[0] + lat_interval[1]) / 2
                if latitude > mid:
                    ch |= (1 << (4 - bits))
                    lat_interval[0] = mid
                else:
                    lat_interval[1] = mid
            
            even_bit = not even_bit
            bits += 1
            
            if bits == 5:
                # We have enough bits for one Base32 character
                geohash.append(cls.BASE32[ch])
                bits = 0
                ch = 0
                
        return "".join(geohash)

# --- Example Usage ---
if __name__ == "__main__":
    # Example: Kotiya chittra, India
    lat, lon = 25.9380, 81.2326
    
    print(f"Coordinates: {lat}, {lon}")
    
    # Generate hashes at different precisions
    for p in [5, 7, 9, 12]:
        ghash = GeohashConverter.encode(lat, lon, precision=p)
        print(f"Precision {p:2}: {ghash}")

    # Visual Guide for precision:
    # 5: ~4.9km x 4.9km
    # 7: ~152m x 152m
    # 9: ~4.8m x 4.8m