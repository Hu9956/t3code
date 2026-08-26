/**
 * AntigravityTextGeneration — thread titles / commit messages via `agy -p`.
 *
 * Uses the CLI's non-interactive JSON mode (`--output-format json`) with a
 * fixed low-cost flash model. One short-lived process per operation.
 *
 * @module textGeneration/AntigravityTextGeneration
 */
import { type AntigravitySettings } from "@t3tools/contracts";
import { sanitizeBranchFragment } from "@t3tools/shared/git";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/unstable/process/ChildProcess";
import { ChildProcessSpawner } from "effect/unstable/process";

import { TextGenerationError } from "@t3tools/contracts";
import * as TextGeneration from "./TextGeneration.ts";
import {
  buildBranchNamePrompt,
  buildCommitMessagePrompt,
  buildPrContentPrompt,
  buildThreadTitlePrompt,
} from "./TextGenerationPrompts.ts";
import {
  sanitizeCommitSubject,
  sanitizePrTitle,
  sanitizeThreadTitle,
} from "./TextGenerationUtils.ts";

const AGY_TIMEOUT_MS = 120_000;
const TEXT_GENERATION_MODEL = "gemini-3.7-flash-low";

interface AgyJsonResult {
  readonly status?: string;
  readonly response?: string;
  readonly error?: string;
}

export const makeAntigravityTextGeneration = Effect.fn("makeAntigravityTextGeneration")(function* (
  antigravitySettings: AntigravitySettings,
  environment: NodeJS.ProcessEnv = process.env,
) {
  const commandSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const command = antigravitySettings.binaryPath?.trim() || "agy";

  const readStreamAsString = (
    operation: string,
    stream: Stream.Stream<Uint8Array, unknown>,
  ): Effect.Effect<string, TextGenerationError> =>
    stream.pipe(
      Stream.decodeText(),
      Stream.runFold(
        () => "",
        (acc: string, chunk: string) => acc + chunk,
      ),
      Effect.mapError(
        (cause) =>
          new TextGenerationError({
            operation,
            detail: "Failed to collect Antigravity CLI output.",
            cause,
          }),
      ),
    );

  const runAgyJson = (input: {
    readonly operation: string;
    readonly cwd: string;
    readonly prompt: string;
  }): Effect.Effect<string, TextGenerationError> =>
    Effect.gen(function* () {
      const child = yield* commandSpawner
        .spawn(
          ChildProcess.make(
            command,
            ["-p", input.prompt, "--model", TEXT_GENERATION_MODEL, "--output-format", "json"],
            { env: environment, extendEnv: true, cwd: input.cwd },
          ),
        )
        .pipe(
          Effect.mapError(
            (cause) =>
              new TextGenerationError({
                operation: input.operation,
                detail: "Failed to spawn the Antigravity CLI.",
                cause,
              }),
          ),
        );
      const [stdout, exitCode] = yield* Effect.all(
        [
          readStreamAsString(input.operation, child.stdout),
          child.exitCode.pipe(
            Effect.map(Number),
            Effect.mapError(
              (cause) =>
                new TextGenerationError({
                  operation: input.operation,
                  detail: "Failed to read the Antigravity CLI exit code.",
                  cause,
                }),
            ),
          ),
        ],
        { concurrency: "unbounded" },
      );

      if (exitCode !== 0) {
        return yield* new TextGenerationError({
          operation: input.operation,
          detail: `Antigravity CLI failed with exit code ${exitCode}.`,
        });
      }

      const parsed = parseAgyJson(stdout);
      if (!parsed || parsed.status !== "SUCCESS" || !parsed.response) {
        return yield* new TextGenerationError({
          operation: input.operation,
          detail: parsed?.error ?? "Antigravity returned no response.",
        });
      }
      return parsed.response.trim();
    }).pipe(
      Effect.scoped,
      Effect.timeoutOption(`${AGY_TIMEOUT_MS} millis`),
      Effect.flatMap(
        Option.match({
          onNone: () =>
            Effect.fail(
              new TextGenerationError({
                operation: input.operation,
                detail: "Antigravity text generation timed out.",
              }),
            ),
          onSome: (generated) => Effect.succeed(generated),
        }),
      ),
    );

  const generateCommitMessage: TextGeneration.TextGeneration["Service"]["generateCommitMessage"] =
    Effect.fn("AntigravityTextGeneration.generateCommitMessage")(function* (input) {
      const { prompt } = buildCommitMessagePrompt({
        branch: input.branch,
        stagedSummary: input.stagedSummary,
        stagedPatch: input.stagedPatch,
        includeBranch: input.includeBranch === true,
        policy: input.policy,
      });
      const raw = yield* runAgyJson({
        operation: "generateCommitMessage",
        cwd: input.cwd,
        prompt,
      });
      const generated = parseLooseJson(raw) as {
        subject?: string;
        body?: string;
        branch?: string;
      };
      return {
        subject: sanitizeCommitSubject(generated.subject ?? raw),
        body: (generated.body ?? "").trim(),
        ...("branch" in generated && typeof generated.branch === "string"
          ? { branch: sanitizeBranchFragment(generated.branch) }
          : {}),
      };
    });

  const generatePrContent: TextGeneration.TextGeneration["Service"]["generatePrContent"] =
    Effect.fn("AntigravityTextGeneration.generatePrContent")(function* (input) {
      const { prompt } = buildPrContentPrompt({
        baseBranch: input.baseBranch,
        headBranch: input.headBranch,
        commitSummary: input.commitSummary,
        diffSummary: input.diffSummary,
        diffPatch: input.diffPatch,
        policy: input.policy,
        changeRequestTemplate: input.changeRequestTemplate,
      });
      const raw = yield* runAgyJson({
        operation: "generatePrContent",
        cwd: input.cwd,
        prompt,
      });
      const generated = parseLooseJson(raw) as { title?: string; body?: string };
      return {
        title: sanitizePrTitle(generated.title ?? raw),
        body: (generated.body ?? "").trim(),
      };
    });

  const generateBranchName: TextGeneration.TextGeneration["Service"]["generateBranchName"] =
    Effect.fn("AntigravityTextGeneration.generateBranchName")(function* (input) {
      const { prompt } = buildBranchNamePrompt({
        message: input.message,
        attachments: input.attachments,
      });
      const raw = yield* runAgyJson({
        operation: "generateBranchName",
        cwd: input.cwd,
        prompt,
      });
      const generated = parseLooseJson(raw) as { branch?: string };
      return { branch: sanitizeBranchFragment(generated.branch ?? raw) };
    });

  const generateThreadTitle: TextGeneration.TextGeneration["Service"]["generateThreadTitle"] =
    Effect.fn("AntigravityTextGeneration.generateThreadTitle")(function* (input) {
      const { prompt } = buildThreadTitlePrompt({
        message: input.message,
        previousTitle: input.previousTitle,
        attachments: input.attachments,
      });
      const raw = yield* runAgyJson({
        operation: "generateThreadTitle",
        cwd: input.cwd,
        prompt,
      });
      const generated = parseLooseJson(raw) as { title?: string };
      return {
        title: sanitizeThreadTitle(generated.title ?? raw),
      } satisfies TextGeneration.ThreadTitleGenerationResult;
    });

  return {
    generateCommitMessage,
    generatePrContent,
    generateBranchName,
    generateThreadTitle,
  } satisfies TextGeneration.TextGeneration["Service"];
});

/** Parse the CLI's top-level JSON envelope; null when malformed. */
function parseAgyJson(raw: string): AgyJsonResult | null {
  try {
    return JSON.parse(raw) as AgyJsonResult;
  } catch {
    return null;
  }
}

/** The model may wrap JSON in prose; extract the first balanced object if present. */
function parseLooseJson(raw: string): Record<string, unknown> {
  const trimmed = raw.trim();
  const start = trimmed.indexOf("{");
  const end = trimmed.lastIndexOf("}");
  if (start >= 0 && end > start) {
    try {
      return JSON.parse(trimmed.slice(start, end + 1)) as Record<string, unknown>;
    } catch {
      // fall through to empty
    }
  }
  return {};
}
