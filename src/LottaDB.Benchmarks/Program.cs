using BenchmarkDotNet.Running;
using Lotta.Benchmarks;

// Run with: dotnet run -c Release -- [filter]
// Examples:
//   dotnet run -c Release                         ? runs all benchmarks (SQLite + Azure)
//   dotnet run -c Release -- --filter "*SQLite*"  ? SQLite only
//   dotnet run -c Release -- --filter "*Azure*"   ? Azure only (requires Azurite / Azure Storage)
BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
