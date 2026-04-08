import os
import urllib.request

JARS = [
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.0/iceberg-spark-runtime-3.5_2.12-1.6.0.jar",
    "https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/0.99.0/nessie-spark-extensions-3.5_2.12-0.99.0.jar",
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar",
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar",
    "https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/1.0.7.Final/wildfly-openssl-1.0.7.Final.jar",
    # JMX Exporter cho Prometheus monitoring (Spark, Trino)
    "https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/1.0.1/jmx_prometheus_javaagent-1.0.1.jar",
]

def main():
    # Thư mục gốc của project
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    jars_dir = os.path.join(project_root, "jars")
    
    os.makedirs(jars_dir, exist_ok=True)
    
    print(f"Checking and downloading jar files to: {jars_dir}")
    for url in JARS:
        filename = url.split("/")[-1]
        filepath = os.path.join(jars_dir, filename)
        if not os.path.exists(filepath):
            print(f"Downloading {filename}...")
            urllib.request.urlretrieve(url, filepath)
            print(f"Downloaded {filename}.")
        else:
            print(f"File {filename} already exists, skipping.")
            
    print("Jar download complete!")

if __name__ == "__main__":
    main()
