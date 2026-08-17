/**
 * Minimal quiz component.
 * Markup:
 * <div class="quiz" data-answer="1">
 *   <div class="quiz-q">Question?</div>
 *   <div class="quiz-opts">
 *     <button type="button" class="opt" data-i="0">...</button>
 *     ...
 *   </div>
 *   <div class="feedback" hidden></div>
 * </div>
 * data-answer is 0-based index. Feedback copy via data-ok / data-no attributes on .quiz
 */
(function () {
  function wire(quiz) {
    const answer = Number(quiz.dataset.answer);
    const ok = quiz.dataset.ok || "Correct.";
    const no = quiz.dataset.no || "Not quite — try again next time; see explanation below.";
    const feedback = quiz.querySelector(".feedback");
    const buttons = [...quiz.querySelectorAll("button.opt")];

    buttons.forEach((btn) => {
      btn.addEventListener("click", () => {
        const i = Number(btn.dataset.i);
        buttons.forEach((b) => {
          b.disabled = true;
          const bi = Number(b.dataset.i);
          if (bi === answer) b.classList.add("correct");
          if (bi === i && i !== answer) b.classList.add("wrong");
        });
        if (feedback) {
          feedback.hidden = false;
          if (i === answer) {
            feedback.className = "feedback ok";
            feedback.textContent = ok;
          } else {
            feedback.className = "feedback no";
            feedback.textContent = no;
          }
        }
      });
    });
  }

  document.querySelectorAll(".quiz").forEach(wire);
})();
