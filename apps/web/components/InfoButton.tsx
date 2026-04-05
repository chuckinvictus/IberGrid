"use client";

import { useState, useRef, useEffect } from "react";

export default function InfoButton({ text }: { text: string }) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLSpanElement>(null);

  useEffect(() => {
    if (!open) return;
    function handleClick(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, [open]);

  return (
    <span className="info-btn-wrapper" ref={ref}>
      <button
        className="info-btn"
        onClick={() => setOpen((o) => !o)}
        aria-label="More information"
        type="button"
      >
        i
      </button>
      {open && <div className="info-popover">{text}</div>}
    </span>
  );
}
