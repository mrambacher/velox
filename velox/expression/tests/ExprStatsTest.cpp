/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <gmock/gmock.h>
#include "velox/core/Expressions.h"
#include "velox/expression/Expr.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/tests/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::test;

class ExprStatsTest : public testing::Test, public VectorTestBase {
 protected:
  void SetUp() override {
    functions::prestosql::registerAllScalarFunctions();
    parse::registerTypeResolver();
  }

  static RowTypePtr asRowType(const TypePtr& type) {
    return std::dynamic_pointer_cast<const RowType>(type);
  }

  std::shared_ptr<const core::ITypedExpr> parseExpression(
      const std::string& text,
      const RowTypePtr& rowType) {
    auto untyped = parse::parseExpr(text);
    return core::Expressions::inferTypes(untyped, rowType, execCtx_->pool());
  }

  std::unique_ptr<exec::ExprSet> compileExpressions(
      const std::vector<std::string>& exprs,
      const RowTypePtr& rowType) {
    std::vector<std::shared_ptr<const core::ITypedExpr>> expressions;
    expressions.reserve(exprs.size());
    for (const auto& expr : exprs) {
      expressions.emplace_back(parseExpression(expr, rowType));
    }
    return std::make_unique<exec::ExprSet>(
        std::move(expressions), execCtx_.get());
  }

  VectorPtr evaluate(exec::ExprSet& exprSet, const RowVectorPtr& input) {
    exec::EvalCtx context(execCtx_.get(), &exprSet, input.get());

    SelectivityVector rows(input->size());
    std::vector<VectorPtr> result(1);
    exprSet.eval(rows, &context, &result);
    return result[0];
  }

  std::shared_ptr<core::QueryCtx> queryCtx_{core::QueryCtx::createForTest()};
  std::unique_ptr<memory::MemoryPool> pool_{
      memory::getDefaultScopedMemoryPool()};
  std::unique_ptr<core::ExecCtx> execCtx_{
      std::make_unique<core::ExecCtx>(pool_.get(), queryCtx_.get())};
};

TEST_F(ExprStatsTest, printWithStats) {
  vector_size_t size = 1'024;

  auto data = makeRowVector({
      makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      makeFlatVector<int32_t>(size, [](auto row) { return row % 7; }),
  });

  auto rowType = asRowType(data->type());
  {
    auto exprSet =
        compileExpressions({"(c0 + 3) * c1", "(c0 + c1) % 2 = 0"}, rowType);

    // Check stats before evaluation.
    ASSERT_EQ(
        exec::printExprWithStats(*exprSet),
        "multiply [cpu time: 0ns, rows: 0] -> BIGINT\n"
        "   plus [cpu time: 0ns, rows: 0] -> BIGINT\n"
        "      cast(c0 as BIGINT) [cpu time: 0ns, rows: 0] -> BIGINT\n"
        "         c0 [cpu time: 0ns, rows: 0] -> INTEGER\n"
        "      3:BIGINT [cpu time: 0ns, rows: 0] -> BIGINT\n"
        "   cast(c1 as BIGINT) [cpu time: 0ns, rows: 0] -> BIGINT\n"
        "      c1 [cpu time: 0ns, rows: 0] -> INTEGER\n"
        "\n"
        "eq [cpu time: 0ns, rows: 0] -> BOOLEAN\n"
        "   mod [cpu time: 0ns, rows: 0] -> BIGINT\n"
        "      cast(plus as BIGINT) [cpu time: 0ns, rows: 0] -> BIGINT\n"
        "         plus [cpu time: 0ns, rows: 0] -> INTEGER\n"
        "            c0 [cpu time: 0ns, rows: 0] -> INTEGER\n"
        "            c1 [cpu time: 0ns, rows: 0] -> INTEGER\n"
        "      2:BIGINT [cpu time: 0ns, rows: 0] -> BIGINT\n"
        "   0:BIGINT [cpu time: 0ns, rows: 0] -> BIGINT\n");

    evaluate(*exprSet, data);

    // Check stats after evaluation.
    ASSERT_THAT(
        exec::printExprWithStats(*exprSet),
        ::testing::MatchesRegex(
            "multiply .cpu time: .+, rows: 1024. -> BIGINT\n"
            "   plus .cpu time: .+, rows: 1024. -> BIGINT\n"
            "      cast.c0 as BIGINT. .cpu time: .+, rows: 1024. -> BIGINT\n"
            "         c0 .cpu time: 0ns, rows: 0. -> INTEGER\n"
            "      3:BIGINT .cpu time: 0ns, rows: 0. -> BIGINT\n"
            "   cast.c1 as BIGINT. .cpu time: .+, rows: 1024. -> BIGINT\n"
            "      c1 .cpu time: 0ns, rows: 0. -> INTEGER\n"
            "\n"
            "eq .cpu time: .+, rows: 1024. -> BOOLEAN\n"
            "   mod .cpu time: .+, rows: 1024. -> BIGINT\n"
            "      cast.plus as BIGINT. .cpu time: .+, rows: 1024. -> BIGINT\n"
            "         plus .cpu time: .+, rows: 1024. -> INTEGER\n"
            "            c0 .cpu time: 0ns, rows: 0. -> INTEGER\n"
            "            c1 .cpu time: 0ns, rows: 0. -> INTEGER\n"
            "      2:BIGINT .cpu time: 0ns, rows: 0. -> BIGINT\n"
            "   0:BIGINT .cpu time: 0ns, rows: 0. -> BIGINT\n"));
  }

  // Use dictionary encoding to repeat each row 5 times.
  auto indices = makeIndices(size, [](auto row) { return row / 5; });
  data = makeRowVector({
      wrapInDictionary(indices, size, data->childAt(0)),
      wrapInDictionary(indices, size, data->childAt(1)),
  });

  {
    auto exprSet =
        compileExpressions({"(c0 + 3) * c1", "(c0 + c1) % 2 = 0"}, rowType);
    evaluate(*exprSet, data);

    ASSERT_THAT(
        exec::printExprWithStats(*exprSet),
        ::testing::MatchesRegex(
            "multiply .cpu time: .+, rows: 205. -> BIGINT\n"
            "   plus .cpu time: .+, rows: 205. -> BIGINT\n"
            "      cast.c0 as BIGINT. .cpu time: .+, rows: 205. -> BIGINT\n"
            "         c0 .cpu time: 0ns, rows: 0. -> INTEGER\n"
            "      3:BIGINT .cpu time: 0ns, rows: 0. -> BIGINT\n"
            "   cast.c1 as BIGINT. .cpu time: .+, rows: 205. -> BIGINT\n"
            "      c1 .cpu time: 0ns, rows: 0. -> INTEGER\n"
            "\n"
            "eq .cpu time: .+, rows: 205. -> BOOLEAN\n"
            "   mod .cpu time: .+, rows: 205. -> BIGINT\n"
            "      cast.plus as BIGINT. .cpu time: .+, rows: 205. -> BIGINT\n"
            "         plus .cpu time: .+, rows: 205. -> INTEGER\n"
            "            c0 .cpu time: 0ns, rows: 0. -> INTEGER\n"
            "            c1 .cpu time: 0ns, rows: 0. -> INTEGER\n"
            "      2:BIGINT .cpu time: 0ns, rows: 0. -> BIGINT\n"
            "   0:BIGINT .cpu time: 0ns, rows: 0. -> BIGINT\n"));
  }
}

struct Event {
  std::string uuid;
  std::unordered_map<std::string, exec::ExprStats> stats;
};

class TestListener : public exec::ExprSetListener {
 public:
  explicit TestListener(std::vector<Event>& events) : events_{events} {}

  void onCompletion(
      const std::string& uuid,
      const exec::ExprSetCompletionEvent& event) override {
    events_.push_back({uuid, event.stats});
  }

 private:
  std::vector<Event>& events_;
};

TEST_F(ExprStatsTest, listener) {
  vector_size_t size = 1'024;

  // Register a listener to receive stats on ExprSet destruction.
  std::vector<Event> events;
  auto listener = std::make_shared<TestListener>(events);
  ASSERT_TRUE(exec::registerExprSetListener(listener));
  ASSERT_FALSE(exec::registerExprSetListener(listener));

  auto data = makeRowVector({
      makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      makeFlatVector<int32_t>(size, [](auto row) { return row % 7; }),
  });

  // Evaluate a couple of expressions and sanity check the stats received by the
  // listener.
  auto rowType = asRowType(data->type());
  {
    auto exprSet =
        compileExpressions({"(c0 + 3) * c1", "(c0 + c1) % 2 = 0"}, rowType);
    evaluate(*exprSet, data);
  }
  ASSERT_EQ(1, events.size());
  auto stats = events.back().stats;
  ASSERT_EQ(1024 * 2, stats.at("plus").numProcessedRows);
  ASSERT_EQ(1024, stats.at("multiply").numProcessedRows);
  ASSERT_EQ(1024, stats.at("mod").numProcessedRows);

  // Evaluate the same expressions twice and verify that stats received by the
  // listener are "doubled".
  {
    auto exprSet =
        compileExpressions({"(c0 + 3) * c1", "(c0 + c1) % 2 = 0"}, rowType);
    evaluate(*exprSet, data);
    evaluate(*exprSet, data);
  }
  ASSERT_EQ(2, events.size());
  stats = events.back().stats;
  ASSERT_EQ(1024 * 2 * 2, stats.at("plus").numProcessedRows);
  ASSERT_EQ(1024 * 2, stats.at("multiply").numProcessedRows);
  ASSERT_EQ(1024 * 2, stats.at("mod").numProcessedRows);

  ASSERT_NE(events[0].uuid, events[1].uuid);

  // Unregister the listener, evaluate expressions again and verify the listener
  // wasn't invoked.
  ASSERT_TRUE(exec::unregisterExprSetListener(listener));
  ASSERT_FALSE(exec::unregisterExprSetListener(listener));

  {
    auto exprSet =
        compileExpressions({"(c0 + 3) * c1", "(c0 + c1) % 2 = 0"}, rowType);
    evaluate(*exprSet, data);
  }
  ASSERT_EQ(2, events.size());
}
