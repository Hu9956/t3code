import { Loader2Icon } from "lucide-react";
import { cn } from "~/lib/utils";
import i18next from "i18next";

function Spinner({ className, ...props }: React.ComponentProps<typeof Loader2Icon>) {
  return (
    <Loader2Icon
      aria-label={i18next.t("Loading")}
      className={cn("animate-spin", className)}
      role="status"
      {...props}
    />
  );
}

export { Spinner };
