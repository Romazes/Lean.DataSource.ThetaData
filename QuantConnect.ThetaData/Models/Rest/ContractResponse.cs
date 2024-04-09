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

using Newtonsoft.Json;
using QuantConnect.Lean.DataSource.ThetaData.Converters;

namespace QuantConnect.Lean.DataSource.ThetaData.Models.Rest
{
    public readonly struct ContractResponse
    {
        [JsonProperty("root")]
        public string Root { get; }

        [JsonProperty("expiration")]
        [JsonConverter(typeof(DateTimeIntJsonConverter))]
        public DateTime ExpirationDate { get; }

        [JsonProperty("strike")]
        public decimal Strike { get; }

        [JsonProperty("right")]
        public string Right { get; }

        [JsonConstructor]
        public ContractResponse(string root, DateTime expirationDate, decimal strike, string right)
        {
            Root = root;
            ExpirationDate = expirationDate;
            Strike = strike;
            Right = right;
        }
    }
}
