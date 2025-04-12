from io import BytesIO
import sys
import tarfile
from mbslave.mbslave.replication import (
    Config,
    MismatchedSchemaError,
    PacketImporter,
    PacketNotFoundError,
    ReplicationHook,
    connect_db,
    download_packet,
    main,
    split_paths,
)


def process_tar(
    fileobj: BytesIO,
    db,
    schema,
    ignored_schemas,
    ignored_tables,
    expected_schema_seq,
    replication_seq,
    hook,
) -> None:
    tar = tarfile.open(fileobj=fileobj, mode="r|*")
    importer = PacketImporter(
        db, schema, ignored_schemas, ignored_tables, replication_seq, hook
    )
    for i, member in enumerate(tar):
        print(f"Processing {i}: {member.name}")
        member_file = tar.extractfile(member)
        if member_file is None:
            continue
        if member.name == "SCHEMA_SEQUENCE":
            schema_seq = int(member_file.read().strip())
            if schema_seq != expected_schema_seq:
                raise MismatchedSchemaError(
                    "Mismatched schema sequence, %d (database) vs %d (replication packet)"
                    % (expected_schema_seq, schema_seq)
                )
        elif member.name == "TIMESTAMP":
            ts = member_file.read().strip().decode("utf8")
            print("Packet was produced at %s", ts)
        elif member.name == "mbdump/pending_data":
            importer.load_pending_data(member_file)
        elif member.name == "mbdump/pending_keys":
            importer.load_pending_keys(member_file)
    importer.process()


def mbslave_sync_main(config: Config) -> None:
    db = connect_db(config)

    base_url = config.musicbrainz.base_url
    token = config.musicbrainz.token
    ignored_schemas = config.schemas.ignored_schemas
    ignored_tables = config.tables.ignored_tables

    hook_class = ReplicationHook

    while True:
        cursor = db.cursor()
        print("Checking for new packets...")
        cursor.execute(
            "SELECT current_schema_sequence, current_replication_sequence FROM %s.replication_control"
            % config.schemas.name("musicbrainz")
        )
        schema_seq, replication_seq = cursor.fetchone()
        replication_seq += 1
        hook = hook_class(config, db, config)
        try:
            print(f"Downloading packet {replication_seq}...")
            with download_packet(base_url, token, replication_seq) as packet:
                print(f"Processing packet {replication_seq}...")
                try:
                    process_tar(
                        packet,
                        db,
                        config,
                        ignored_schemas,
                        ignored_tables,
                        schema_seq,
                        replication_seq,
                        hook,
                    )
                    print(f"Processed packet {replication_seq}")
                except MismatchedSchemaError:
                    raise
        except PacketNotFoundError:
            print("Not found, stopping")
            break
        sys.stdout.flush()
        sys.stderr.flush()


mbslave_sync_main(Config(split_paths("mbslave.conf")))
