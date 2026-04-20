using System;
using System.Collections.Generic;
using System.Text;

namespace Lotta.Tests
{
    public static class Extensions
    {
        public static ILottaConfiguration ConfigureTestStorage(this ILottaConfiguration config)
        {
            //config.TableServiceClientFactory = LottaDBFixture.CreateMockTableServiceClient;
            //config.LuceneDirectoryFactory = LottaDBFixture.CreateMockDirectory;
            return config;
        }
    }
}
