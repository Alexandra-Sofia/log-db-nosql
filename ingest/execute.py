import os
from multiprocessing import Process

from config import (
    ACCESS_FILENAME,
    DATAX_FILENAME,
    NAMESYS_FILENAME,
    BATCH_SIZE_DEFAULT,
)
from util import tiny_logger
from workers.access_worker import parse_access_worker
from workers.dataxceiver_worker import parse_dataxceiver_worker
from workers.namesystem_worker import parse_namesystem_worker

def main(
    logdir: str = os.getenv("LOG_DIR"),
    mongo_uri: str = os.getenv("MONGO_URI"),
    mongo_db: str = os.getenv("MONGO_DB"),
    mongo_coll: str = os.getenv("MONGO_COLLECTION"),
    batch_size: int = BATCH_SIZE_DEFAULT,
) -> None:

    access_log = os.path.join(logdir, ACCESS_FILENAME)
    datax_log = os.path.join(logdir, DATAX_FILENAME)
    namesys_log = os.path.join(logdir, NAMESYS_FILENAME)

    tiny_logger("[INGEST] Starting workers...")

    p1 = Process(
        target=parse_access_worker,
        args=(access_log, mongo_uri, mongo_db, mongo_coll, batch_size),
    )
    p2 = Process(
        target=parse_dataxceiver_worker,
        args=(datax_log, mongo_uri, mongo_db, mongo_coll, batch_size),
    )
    p3 = Process(
        target=parse_namesystem_worker,
        args=(namesys_log, mongo_uri, mongo_db, mongo_coll, batch_size),
    )

    p1.start()
    p2.start()
    p3.start()

    p1.join()
    p2.join()
    p3.join()

    tiny_logger("[INGEST] All workers completed.")

if __name__ == "__main__":
    main()
