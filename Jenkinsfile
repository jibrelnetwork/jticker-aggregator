builder(
        jUnitReportsPath: 'junit-reports',
        coverageReportsPath: 'coverage-reports',
        buildTasks: [
                [
                        name: "Linters",
                        type: "lint",
                        method: "inside",
                        runAsUser: "root",
                        entrypoint: "",
                        jUnitPath: '/junit-reports',
                        command: [
                                'pip install --no-cache-dir -r requirements-dev.txt',
                                'mkdir -p /junit-reports',
                                'pylama',
                                'mypy --junit-xml=/junit-reports/mypy-junit-report.xml .',
                        ],
                ],
                [
                        name: 'Tests',
                        type: 'test',
                        method: 'inside',
                        runAsUser: 'root',
                        entrypoint: '',
                        jUnitPath: '/junit-reports',
                        coveragePath: '/coverage-reports',
                        environment: [
                                KAFKA_BOOTSTRAP_SERVERS: "kafka:9092",
                                INFLUX_HOST: 'influxdb',
                                INFLUX_DB: 'jticker_tests',
                        ],
                        sidecars: [
                                zookeeper: [
                                        image: "zookeeper"
                                ],
                                kafka: [
                                        image: 'wurstmeister/kafka',
                                        environment: [
                                                KAFKA_BROKER_ID: 0,
                                                KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181',
                                                KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://kafka:9092',
                                                KAFKA_LISTENERS: 'PLAINTEXT://:9092',
                                                KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'PLAINTEXT:PLAINTEXT',
                                                KAFKA_LOG_CLEANUP_POLICY: 'compact',
                                        ],
                                ],
                                influxdb: [
                                        image: 'influxdb:1.7-alpine',
                                        environment: [
                                                INFLUXDB_DB: 'jticker_tests',
                                        ]
                                ],
                        ],
                        command: [
                                'pip install --no-cache-dir -r requirements-dev.txt',
                                'mkdir -p /junit-reports',
                                'pytest --junitxml=/junit-reports/pytest-junit-report.xml --cov=jticker_aggregator --cov-report xml:/coverage-reports/pytest-coverage-report.xml',
                        ],
                ]
        ],
)
