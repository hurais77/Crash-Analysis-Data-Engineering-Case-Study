import argparse
from pyspark.sql import SparkSession
from utils.utils import read_yaml_file
from analysis.crash_analysis import CrashAnalysis


def main():
    # Parse arguments
    parser = argparse.ArgumentParser(description="Run specific crash analytics.")
    parser.add_argument(
        "--analysis",
        type=int,
        help="Specify the analysis number to run (1-10). If not provided, all analyses will be run.",
        default=None,
    )
    args = parser.parse_args()

    # Initialize Spark session
    spark = SparkSession.builder.appName("Crash Analytics").getOrCreate()

    # Load configuration
    config = read_yaml_file("config.yaml")

    # Initialize CrashAnalytics object
    analytics = CrashAnalysis(spark, config)

    # Get all userdefined methods
    analysis_methods = [
        method_name
        for method_name in vars(CrashAnalysis)
        if callable(getattr(CrashAnalysis, method_name)) and not method_name.startswith("_")
    ]

    # Perform analysis based on the argument
    if args.analysis:
        if args.analysis < 1 or args.analysis > len(analysis_methods):
            raise ValueError(f"Invalid analysis number. Please provide a value between 1 and {len(analysis_methods)}.")
        method_name = analysis_methods[args.analysis-1]
        print(f"Running analysis {args.analysis}: {method_name}...")
        getattr(analytics, method_name)()  # Dynamically call the method
    else:
        print("Running all analyses...")
        all_results = []
        for idx, method_name in enumerate(analysis_methods, start=1):
            print(f"Running analysis {idx}: {method_name}...")
            result = getattr(analytics, method_name)()  # Dynamically call each method
            all_results.append(result)
        
        for idx, res in enumerate(all_results):
            print(f"Result {idx+1} : {res}")

    # Stop Spark session
    spark.stop()


if __name__ == "__main__":
    main()
