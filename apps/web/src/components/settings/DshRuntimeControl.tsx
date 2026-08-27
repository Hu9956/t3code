"use client";

import { useCallback, useState } from "react";
import { useTranslation } from "react-i18next";
import { LoaderIcon, PowerIcon, SquareIcon } from "lucide-react";
import type { EnvironmentId, ServerProvider } from "@t3tools/contracts";
import {
  isAtomCommandInterrupted,
  squashAtomCommandFailure,
} from "@t3tools/client-runtime/state/runtime";
import { cn } from "../../lib/utils";
import { Button } from "../ui/button";
import { stackedThreadToast, toastManager } from "../ui/toast";
import { serverEnvironment } from "../../state/server";
import { useAtomCommand } from "../../state/use-atom-command";
import { useEnvironmentQuery } from "../../state/query";

/**
 * DSH 一键启停（3080）— 仅在 DSH 引擎卡出现
 * - 监听状态复用 DshRuntime 的探活（POST /api/host.describe），同时回退到 liveProvider.status === "ready"
 * - 未起显示“启动 DSH”，已起显示“停止 DSH”，点击调后端 server.dsh.* RPC
 * - 状态通过 WS 推送：后端 start/stop 后会 providerRegistry.refresh() → providerStatuses 流推送；
 *   本组件另以 2s stale 的 dshStatus 查询 + 手动 refresh 保底
 */
export function DshRuntimeControl({
  environmentId,
  liveProvider,
}: {
  readonly environmentId: EnvironmentId;
  readonly liveProvider?: ServerProvider | undefined;
}) {
  const { t } = useTranslation();
  const [isBusy, setIsBusy] = useState(false);

  const dshStatusAtom = serverEnvironment.dshStatus({ environmentId, input: {} });
  const {
    data: dshStatus,
    error: dshError,
    refresh: refreshDshStatus,
  } = useEnvironmentQuery(dshStatusAtom);

  const dshStart = useAtomCommand(serverEnvironment.dshStart, { reportFailure: false });
  const dshStop = useAtomCommand(serverEnvironment.dshStop, { reportFailure: false });

  // 3080 是否监听：优先 DshRuntime 探活，回退到 DshDriver 的 provider 探活（ready）
  const listening = dshStatus?.listening ?? liveProvider?.status === "ready";
  const runningPid = dshStatus?.pid ?? null;
  const harnessDir = dshStatus?.harnessDir ?? null;

  const handleToggle = useCallback(async () => {
    if (isBusy) return;
    setIsBusy(true);
    try {
      const result = listening
        ? await dshStop({ environmentId, input: {} })
        : await dshStart({ environmentId, input: {} });

      if (result._tag === "Failure") {
        if (isAtomCommandInterrupted(result)) return;
        const err = squashAtomCommandFailure(result);
        const msg = err instanceof Error ? err.message : String(err);
        toastManager.add(
          stackedThreadToast({
            type: "error",
            title: listening ? t("无法停止 DSH") : t("无法启动 DSH"),
            description: msg,
          }),
        );
      } else {
        toastManager.add({
          type: "success",
          title: listening ? t("DSH 已停止") : t("DSH 已启动"),
          description: listening
            ? t("3080 已释放")
            : t("正在连接 3080… 若 3080 已被外部进程占用，将直接复用"),
        });
      }
    } finally {
      setIsBusy(false);
      // WS 会推送 providerStatuses；这里顺带刷新 dshStatus 以更快收敛
      refreshDshStatus();
      // 小延时后再刷一次，等待 DSH 进程真正监听（cold start 约 1-2s）
      setTimeout(() => refreshDshStatus(), 1200);
      setTimeout(() => refreshDshStatus(), 3000);
    }
  }, [dshStart, dshStop, environmentId, isBusy, listening, refreshDshStatus, t]);

  return (
    <div className="flex flex-col gap-2 rounded-xl border border-border/60 bg-muted/10 px-3 py-2.5">
      <div className="flex items-center gap-2">
        <span
          className={cn(
            "size-2 shrink-0 rounded-full",
            listening
              ? "bg-emerald-500 shadow-[0_0_6px_theme(colors.emerald.500/60)]"
              : "bg-muted-foreground/40",
          )}
          aria-hidden
        />
        <span className="text-xs font-medium text-foreground">
          {listening ? t("DSH 运行中") : t("DSH 未运行")}
          <span className="font-normal text-muted-foreground"> · 3080</span>
        </span>
        {runningPid ? (
          <span className="ml-2 hidden text-[11px] text-muted-foreground sm:inline">
            PID {runningPid}
          </span>
        ) : null}
        {harnessDir ? (
          <span
            className="ml-auto hidden max-w-[220px] truncate text-[11px] text-muted-foreground/70 sm:inline"
            title={harnessDir}
          >
            {harnessDir}
          </span>
        ) : null}
        <Button
          type="button"
          size="xs"
          variant={listening ? "outline" : "default"}
          className="ml-auto sm:ml-2"
          disabled={isBusy}
          onClick={() => void handleToggle()}
          aria-label={listening ? t("停止 DSH") : t("启动 DSH")}
        >
          {isBusy ? (
            <LoaderIcon className="size-3 animate-spin" />
          ) : listening ? (
            <SquareIcon className="size-3" />
          ) : (
            <PowerIcon className="size-3" />
          )}
          {listening ? t("停止 DSH") : t("启动 DSH")}
        </Button>
      </div>
      {dshStatus?.lastError ? (
        <p className="text-xs leading-snug text-destructive/80">{dshStatus.lastError}</p>
      ) : null}
      {dshError ? <p className="text-xs leading-snug text-destructive/80">{dshError}</p> : null}
      {!listening && dshStatus && !dshStatus.running ? (
        <p className="text-[11px] leading-snug text-muted-foreground">
          {t("点击启动将在 deepseek-harness 目录执行 pnpm dsh web --no-open。已运行时将直接复用。")}
        </p>
      ) : null}
    </div>
  );
}
