## 4-byte ID Vs 16-byte ID and Index Size:

Source Code : *id_4byte_vs_16_byte.py*

|Table Definition |time to insert 10 million records | 
|-----------------------------------------------------------------------------------|-------------------|
| CREATE TABLE TableId4Byte ( ID INT AUTO_INCREMENT PRIMARY KEY, Age INT NOT NULL)  | 120.62 seconds|
| CREATE TABLE IF NOT EXISTS TableId16Byte ( ID VARCHAR(16) PRIMARY KEY, Age INT NOT NULL) | 1324.83 seconds |

|Index   |Index size in. bytes | 
|-----------------------------------------------------------------------------------|-------------------|
| CREATE INDEX idx_age ON TableId4Byte (Age)  | 148652032 bytes|
| CREATE INDEX idx_age ON TableId4Byte (Age) | 387973120 bytes |

## Flickr Ticketing Service and Id generation

Source Code : *Flickr_Ticketing_Service_id_generation.py*




