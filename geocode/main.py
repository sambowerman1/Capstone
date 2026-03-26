#!/usr/bin/env python3
"""
Memorial Highway Geocoding Pipeline

Usage:
    python main.py                    # Run full pipeline
    python main.py --phase 1          # Run only Phase 1 (normalize)
    python main.py --phase 1b         # Run only Phase 1b (LLM extraction)
    python main.py --phase 2          # Run only Phase 2 (geocode)
    python main.py --phase 3          # Run only Phase 3 (route tracing)
    python main.py --phase 4          # Run only Phase 4 (centroid + QA)
    python main.py --export           # Export only
    python main.py --stats            # Print status counts
    python main.py --retry-failed     # Re-attempt all failed rows
"""

import argparse
import sys

import db


def print_stats():
    stats = db.get_stats()
    print(f"\nTotal rows: {stats['total']}")
    print("\nBy status:")
    for status, count in sorted(stats["by_status"].items(), key=lambda x: x[0]):
        pct = count / stats["total"] * 100 if stats["total"] else 0
        print(f"  {status:20s} {count:>5d}  ({pct:.1f}%)")
    print("\nBy tier:")
    for tier, count in sorted(stats["by_tier"].items(), key=lambda x: (x[0] is None, x[0])):
        label = f"Tier {tier}" if tier is not None else "Unclassified"
        pct = count / stats["total"] * 100 if stats["total"] else 0
        print(f"  {label:20s} {count:>5d}  ({pct:.1f}%)")


def retry_failed():
    """Reset all failed rows to 'pending' so they re-enter the pipeline."""
    failed = db.get_rows_by_status("failed")
    if not failed:
        print("No failed rows to retry.")
        return
    for row in failed:
        db.update_row(row["id"], status="pending", error_notes=None, confidence=None)
    print(f"Reset {len(failed)} failed rows to 'pending'.")


def run_phase(phase: str):
    if phase == "1":
        import phase1_normalize
        phase1_normalize.run()
    elif phase == "1b":
        import phase1b_extract
        phase1b_extract.run()
    elif phase == "2":
        import phase2_geocode
        phase2_geocode.run()
    elif phase == "3":
        import phase3_routes
        phase3_routes.run()
    elif phase == "4":
        import phase4_fallback
        phase4_fallback.run()
    else:
        print(f"Unknown phase: {phase}")
        sys.exit(1)


def run_full():
    """Run the complete pipeline in order."""
    import phase1_normalize
    import phase1b_extract
    import phase2_geocode
    import phase3_routes
    import phase4_fallback
    import export as export_mod

    print("=" * 60)
    print("Memorial Highway Geocoding Pipeline")
    print("=" * 60)

    phase1_normalize.run()
    print()
    phase1b_extract.run()
    print()
    phase2_geocode.run()
    print()
    phase3_routes.run()
    print()
    phase4_fallback.run()
    print()
    export_mod.run()
    print()
    print_stats()


def main():
    parser = argparse.ArgumentParser(description="Memorial Highway Geocoding Pipeline")
    parser.add_argument("--phase", type=str, help="Run a single phase (1, 1b, 2, 3, 4)")
    parser.add_argument("--export", action="store_true", help="Export results only")
    parser.add_argument("--stats", action="store_true", help="Print pipeline statistics")
    parser.add_argument("--retry-failed", action="store_true", help="Reset failed rows for retry")
    args = parser.parse_args()

    # Always init DB and import CSV
    db.init_db()
    db.import_csv()

    if args.stats:
        print_stats()
        return

    if args.retry_failed:
        retry_failed()
        return

    if args.export:
        import export as export_mod
        export_mod.run()
        return

    if args.phase:
        run_phase(args.phase)
        return

    run_full()


if __name__ == "__main__":
    main()
