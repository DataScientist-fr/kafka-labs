"""Configuration pytest pour les tests d'acceptation du lab L2."""


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "acceptance: test d'acceptation boîte noire (cluster Kafka + Schema Registry requis)",
    )
