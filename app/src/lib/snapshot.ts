/**
 * Snapshot data layer: load and query the observatory JSON snapshot at build time.
 * Safe for Astro SSR/static build (Node-only; no browser APIs).
 */

import { readFileSync, existsSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

// ---------------------------------------------------------------------------
// Types (match data/latest.json and data/YYYY-MM-DD.json schema)
// ---------------------------------------------------------------------------

/** Build/metadata for the snapshot. */
export interface SnapshotMeta {
  generated_at: string;
  ckan_base_url: string;
  ckan_api_path: string;
  freshness_thresholds_days: {
    green_lt: number;
    yellow_lte: number;
  };
  note?: string;
}

/** Global counts across the catalog. */
export interface SnapshotSummary {
  datasets_total: number;
  datasets_green: number;
  datasets_yellow: number;
  datasets_red: number;
  datasets_unknown: number;
  resources_total: number;
  resources_broken: number;
  resources_parse_failed: number;
  errors_total: number;
}

/** Organization (publisher) summary. Slug: `name`. */
export interface SnapshotOrganization {
  id: string;
  name: string;
  title: string;
  datasets_total: number;
  datasets_green: number;
  datasets_yellow: number;
  datasets_red: number;
  datasets_unknown: number;
  resources_total: number;
  resources_broken: number;
  resources_parse_failed: number;
}

/** Resource availability/parse check result. */
export interface ResourceCheck {
  ok: boolean;
  http_status: number;
  error: string | null;
  bytes_read: number;
  parse_ok: boolean | null;
  parse_error: string | null;
  checksum: string;
}

/** A single resource (file) in a dataset. */
export interface SnapshotResource {
  id: string;
  name: string;
  format: string;
  url: string;
  last_modified: string;
  check: ResourceCheck;
}

/** Embedded org reference on a dataset. */
export interface SnapshotOrganizationRef {
  id: string;
  name: string;
  title: string;
}

/** Dataset with resources. Slug: `name`. */
export interface SnapshotDataset {
  id: string;
  name: string;
  title: string;
  organization: SnapshotOrganizationRef;
  last_modified: string;
  days_since_modified: number;
  freshness_bucket: "green" | "yellow" | "red" | "unknown";
  resources_total: number;
  resources_broken: number;
  resources_parse_failed: number;
  formats: string[];
  resources: SnapshotResource[];
  catalog_url: string;
}

/** Root snapshot structure. */
export interface SnapshotRoot {
  meta: SnapshotMeta;
  summary: SnapshotSummary;
  organizations: SnapshotOrganization[];
  datasets: SnapshotDataset[];
}

/** Freshness bucket counts (for charts/summary). */
export interface FreshnessCounts {
  green: number;
  yellow: number;
  red: number;
  unknown: number;
}

// ---------------------------------------------------------------------------
// Snapshot path (relative to this file: app/src/lib → repo/data)
// ---------------------------------------------------------------------------
//
// The daily GitHub Action (e.g. .github/workflows/daily.yml) runs the snapshot
// pipeline and writes the result to data/latest.json. The build uses that file
// when present; otherwise it falls back to data/2026-02-25.json so local and
// PR builds remain deterministic without requiring a prior pipeline run.

const DATA_DIR_REL = ["..", "..", "..", "data"];
const FALLBACK_FILE = "2026-02-25.json";

function getSnapshotPath(): string {
  const dir = path.dirname(fileURLToPath(import.meta.url));
  const dataDir = path.join(dir, ...DATA_DIR_REL);
  const latestPath = path.join(dataDir, "latest.json");
  if (existsSync(latestPath)) {
    return latestPath;
  }
  return path.join(dataDir, FALLBACK_FILE);
}

// ---------------------------------------------------------------------------
// Load
// ---------------------------------------------------------------------------

/** Load the snapshot from disk. Call at build time (e.g. in Astro pages/layouts). */
export function loadSnapshot(): SnapshotRoot {
  const filePath = getSnapshotPath();
  const raw = readFileSync(filePath, "utf-8");
  const data = JSON.parse(raw) as SnapshotRoot;
  return data;
}

// ---------------------------------------------------------------------------
// Queries (all use stable slugs: organization.name, dataset.name)
// ---------------------------------------------------------------------------

/** All organizations. */
export function getOrganizations(snapshot: SnapshotRoot): SnapshotOrganization[] {
  return snapshot.organizations;
}

/** All datasets. */
export function getDatasets(snapshot: SnapshotRoot): SnapshotDataset[] {
  return snapshot.datasets;
}

/** Organization by slug (`name`). */
export function getOrganizationBySlug(
  snapshot: SnapshotRoot,
  orgName: string
): SnapshotOrganization | undefined {
  return snapshot.organizations.find((o) => o.name === orgName);
}

/** Dataset by slug (`name`). */
export function getDatasetBySlug(
  snapshot: SnapshotRoot,
  datasetName: string
): SnapshotDataset | undefined {
  return snapshot.datasets.find((d) => d.name === datasetName);
}

/** Datasets with the most resource problems (broken + parse failed), descending. */
export function getTopDatasetsByProblems(
  snapshot: SnapshotRoot,
  limit = 10
): SnapshotDataset[] {
  const withProblems = [...snapshot.datasets].sort((a, b) => {
    const problemsA = a.resources_broken + a.resources_parse_failed;
    const problemsB = b.resources_broken + b.resources_parse_failed;
    return problemsB - problemsA;
  });
  return withProblems.slice(0, limit);
}

/** Freshness bucket counts from the snapshot summary. */
export function getFreshnessCounts(snapshot: SnapshotRoot): FreshnessCounts {
  const s = snapshot.summary;
  return {
    green: s.datasets_green,
    yellow: s.datasets_yellow,
    red: s.datasets_red,
    unknown: s.datasets_unknown,
  };
}
