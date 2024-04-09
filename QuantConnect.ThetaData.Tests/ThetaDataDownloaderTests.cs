/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/


using System;
using System.Linq;
using NUnit.Framework;
using QuantConnect.Util;
using System.Collections.Generic;

namespace QuantConnect.Lean.DataSource.ThetaData.Tests
{

    [TestFixture]
    [Explicit("This test requires the ThetaData terminal to be running in order to execute properly.")]
    public class ThetaDataDownloaderTests
    {
        private CustomThetaDataDownloader _dataDownloader;

        [SetUp]
        public void SetUp()
        {
            _dataDownloader = new();
        }

        [TearDown]
        public void TearDown()
        {
            if (_dataDownloader != null)
            {
                _dataDownloader.DisposeSafely();
            }
        }

        [TestCase("AAPL", OptionRight.Call, 170, "2024/03/28", Resolution.Tick, TickType.Quote, "2024/03/19", "2024/03/28")]
        [TestCase("AAPL", OptionRight.Call, 170, "2024/03/28", Resolution.Tick, TickType.Trade, "2024/03/19", "2024/03/28")]
        [TestCase("AAPL", OptionRight.Put, 170, "2024/03/28", Resolution.Second, TickType.Quote, "2024/03/19", "2024/03/28")]
        [TestCase("AAPL", OptionRight.Put, 170, "2024/03/28", Resolution.Second, TickType.Trade, "2024/03/19", "2024/03/28")]
        [TestCase("AAPL", OptionRight.Call, 170, "2024/03/28", Resolution.Hour, TickType.Quote, "2024/03/19", "2024/03/28")]
        [TestCase("AAPL", OptionRight.Call, 170, "2024/03/28", Resolution.Hour, TickType.Trade, "2024/03/19", "2024/03/28")]
        [TestCase("AAPL", OptionRight.Put, 170, "2024/03/28", Resolution.Daily, TickType.Quote, "2024/01/18", "2024/03/28")]
        [TestCase("AAPL", OptionRight.Call, 170, "2024/03/28", Resolution.Daily, TickType.Trade, "2024/01/18", "2024/03/28")]
        [TestCase("AAPL", OptionRight.Put, 170, "2024/03/28", Resolution.Daily, TickType.OpenInterest, "2024/01/18", "2024/03/28")]
        public void DownloadsOptionHistoricalData(string ticker, OptionRight optionRight, decimal strikePrice, DateTime expirationDate, Resolution resolution, TickType tickType, DateTime startDate, DateTime endDate)
        {
            var symbol = TestHelpers.CreateSymbol(ticker, SecurityType.Option, optionRight, strikePrice, expirationDate);

            var parameters = new DataDownloaderGetParameters(symbol, resolution, startDate, endDate, tickType);

            var downloadedHistoricalData = _dataDownloader.Get(parameters);

            TestHelpers.ValidateHistoricalBaseData(downloadedHistoricalData, resolution, tickType, startDate, endDate, symbol);
        }

        [TestCase("AAPL", Resolution.Daily, TickType.Quote, "2024/01/18", "2024/03/28")]
        public void DownloadsCanonicalOptionHistoricalData(string ticker, Resolution resolution, TickType tickType, DateTime startDate, DateTime endDate)
        {
            var symbol = Symbol.CreateCanonicalOption(TestHelpers.CreateSymbol(ticker, SecurityType.Equity));

            var parameters = new DataDownloaderGetParameters(symbol, resolution, startDate, endDate, tickType);

            var downloadedData = _dataDownloader.Get(parameters);

            TestHelpers.ValidateHistoricalBaseData(downloadedData, resolution, tickType, startDate, endDate);
        }

        [TestCase("NVDA", Resolution.Minute, TickType.Quote, "2024/01/02", "2024/04/08")]
        [TestCase("NVDA", Resolution.Minute, TickType.Quote, "2024/01/02", "2024/02/02")]
        public void GetBulkOptionContracts(string ticker, Resolution resolution, TickType tickType, DateTime startDate, DateTime endDate)
        {
            var symbol = TestHelpers.CreateSymbol(ticker, SecurityType.Equity);
            var canonicalOption = Symbol.CreateCanonicalOption(symbol);

            var parameters = new DataDownloaderGetParameters(canonicalOption, resolution, startDate, endDate, tickType);

            var downloadedHistoricalData = _dataDownloader.Get(parameters).ToList();

            Assert.IsNotEmpty(downloadedHistoricalData);
        }

        [TestCase("NVDA", "2024/01/02", "2024/04/08", 36)]
        [TestCase("NVDA", "2024/01/02", "2024/02/01", 26)]
        public void GetOptionContracts(string ticker, DateTime startDate, DateTime endData, int expectedAmount)
        {
            var symbol = Symbol.CreateCanonicalOption(TestHelpers.CreateSymbol(ticker, SecurityType.Equity));

            var contracts = _dataDownloader.GetOptionContracts(symbol, startDate, endData).ToList();

            var contractByDate = contracts.OrderBy(x => x.ID.Date).Select(x => x.ID.Date).Distinct().ToList();

            Assert.IsNotEmpty(contracts);
            Assert.IsNotEmpty(contractByDate);
            Assert.That(contracts.Count, Is.EqualTo(expectedAmount));
            Assert.That(contractByDate.Count, Is.EqualTo(expectedAmount));
            Assert.That(contractByDate.Count, Is.EqualTo(contracts.Count));
        }

        public class CustomThetaDataDownloader : ThetaDataDownloader
        {
            public IEnumerable<Symbol> GetOptionContracts(Symbol symbol, DateTime startUtc, DateTime endUtc)
            {
                return base.GetOptions(symbol, startUtc, endUtc);
            }
        }
    }
}
