from setup.redis_setup import constants

configurations = {
    constants.REDIS_SERVER: "localhost",
    constants.REDIS_PORT: 6379,
    constants.DUPLICATE_TTL: 60,
    constants.SET_NAME : "conversion_ids"
}
