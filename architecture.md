```mermaid
flowchart TD
    subgraph Producer["Data Producer (EC2)"]
        DP[dataFeedMsk.py]
    end

    subgraph MSK["Amazon MSK (Kafka)"]
        KT[Kafka Topics]
    end

    subgraph Processing["Stream Processing"]
        subgraph Flink["Apache Flink Application"]
            SF[SourceFactory]
            SP[StreamProcessor]
            SK[SinkFactory]
            CL[ConfigLoader]
            
            SF --> SP
            SP --> SK
            CL --> SF
            CL --> SK
        end
    end

    subgraph Consumer["Data Consumer (EC2)"]
        DC[Kafka Consumer]
    end

    DP -->|Produces Data| KT
    KT -->|Input Stream| SF
    SK -->|Output Stream| Consumer

    classDef aws fill:#FF9900,stroke:#232F3E,stroke-width:2px,color:white;
    classDef app fill:#009639,stroke:#232F3E,stroke-width:2px,color:white;
    
    class MSK aws;
    class Producer,Consumer app;
    class Flink app;
