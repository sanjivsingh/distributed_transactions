local driver_locations_key = KEYS[1]
local driver_metadata_key = KEYS[2]

local longitude_str = ARGV[1]
local latitude_str = ARGV[2]
local radius_str = ARGV[3]
local max_count_str = ARGV[4]
local required_car_type = ARGV[5]
local required_payment_pref = ARGV[6]

-- Validate and convert arguments
if not longitude_str or longitude_str == "" then
    return {}
end

local longitude = tonumber(longitude_str)
local latitude = tonumber(latitude_str)
local radius = tonumber(radius_str)
local max_count = tonumber(max_count_str)

-- Validate converted numbers
if not longitude or not latitude or not radius or not max_count then
    return {}
end

-- Find nearby drivers using GEORADIUS
local nearby_drivers = redis.call('GEORADIUS', driver_locations_key, longitude, latitude, radius, 'mi', 'WITHDIST', 'WITHCOORD', 'ASC', 'COUNT', max_count * 2)

local matching_drivers = {}
local count = 0

-- GEORADIUS returns: [[driver_id, [distance], [lng, lat]], ...]
for i = 1, #nearby_drivers do
    if count >= max_count then
        break
    end
    local driver_result = nearby_drivers[i]
    local driver_id = driver_result[1]
    local distance = driver_result[2]
    local coords = driver_result[3]  -- [lng, lat]
    
    -- Get driver metadata
    local metadata_json = redis.call('HGET', driver_metadata_key, driver_id)
    
    if metadata_json then
        -- Improved JSON parsing using pattern matching
        -- Look for "car_type":"value" pattern (handles quotes properly)
        local car_type_pattern = '"car_type"%s*:%s*"([^"]*)"'
        local found_car_type = string.match(metadata_json, car_type_pattern)
        local car_type_match = found_car_type == required_car_type
        
        -- Look for payment preference pattern
        local payment_pattern = '"payment_preference"%s*:%s*"([^"]*)"'
        local found_payment_pref = string.match(metadata_json, payment_pattern)
        local payment_match = false
        
        if required_payment_pref == 'both' then
            payment_match = true
        elseif found_payment_pref then
            payment_match = (found_payment_pref == required_payment_pref) or (found_payment_pref == 'both')
        end
        
        -- Look for status pattern
        local status_pattern = '"status"%s*:%s*"([^"]*)"'
        local found_status = string.match(metadata_json, status_pattern)
        local status_available = found_status == 'available'
        
        if car_type_match and payment_match and status_available then
            table.insert(matching_drivers, driver_id)
            table.insert(matching_drivers, distance)
            table.insert(matching_drivers, coords[1]) -- longitude
            table.insert(matching_drivers, coords[2]) -- latitude
            table.insert(matching_drivers, metadata_json)
            count = count + 1
        else
            -- Skip this driver as they do not match criteria
        end
    end
end

return matching_drivers