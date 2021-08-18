TEST_INGEST_OUTPUT_EXPECTED = [
    ["tt0000487", "The Great Train Robbery", "original", None, 3, None, 1, None, 1621806662, 20210603],
    [
        "tt0000239",
        "Danse serpentine par Mme. Bob Walter",
        None,
        "CN",
        2,
        None,
        0,
        None,
        1621806634,
        20210603,
    ],
    ["tt0000417", "En Tur til Maanen", "imdbDisplay", "AR", 13, None, 0, None, 1621806635, 20210603],
]

TEST_TRANSFORM_OUTPUT_EXPECTED = [
    [
        "tt0000487",
        "The Great Train Robbery",
        "original",
        None,
        3,
        None,
        True,
        None,
        "long",
        1621806662,
        20210603,
    ]
]
