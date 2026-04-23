import React from "https://esm.sh/react@18.3.1";
import { ANALYTIC_QUESTIONS } from "../models/analyticsQuestionsModel.js";

export function QuestionCards() {
  return React.createElement(
    "section",
    { className: "question-grid", "aria-label": "Perguntas analíticas" },
    ANALYTIC_QUESTIONS.map((question) =>
      React.createElement(
        "article",
        { className: "question-card", key: question.key },
        React.createElement("h3", null, question.title),
        React.createElement("p", null, question.description),
      ),
    ),
  );
}
