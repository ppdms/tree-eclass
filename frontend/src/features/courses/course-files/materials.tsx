import * as stylex from '@stylexjs/stylex';
import * as React from 'react';
import { BookOpenCheck, BookOpenText, Download, FileClock, FileText } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { fetchJson } from '@/lib/api';
import type { ExternalMaterial, ExternalMaterialType } from '@/lib/types';
import { colors, layout, typography } from '@/styles/tokens.stylex';
import { formatFileSize, formatTimestamp } from './helpers';
import {
  classificationLabel,
  MATERIAL_TYPES,
  materialMatches,
  materialTypeValue,
  statusLabel,
  TYPE_META,
} from './materialHelpers';
import { MaterialUploadPanel } from './MaterialUploadPanel';

const styles = stylex.create({
  externalMaterials: {
    gap: '1rem',
    display: 'grid',
    marginTop: '1.5rem',
  },
  externalHeader: {
    gap: '.75rem',
    alignItems: 'flex-start',
    display: 'flex',
    borderTopColor: colors.border,
    borderTopStyle: 'solid',
    borderTopWidth: 1,
    paddingTop: '1.5rem',
  },
  headerIcon: {
    color: colors.primary,
    height: '1.25rem',
    marginTop: '.125rem',
    width: '1.25rem',
  },
  headerTitle: {
    margin: 0,
    fontSize: typography.sizeBase,
    fontWeight: typography.weightSemibold,
  },
  headerDesc: {
    margin: 0,
    color: colors.textSecondary,
    fontSize: '0.75rem',
    marginTop: '.25rem',
  },
  groupSection: {
    borderColor: colors.border,
    borderRadius: layout.radiusLarge,
    borderStyle: 'solid',
    borderWidth: 1,
    overflow: 'hidden',
    backgroundColor: colors.surface,
  },
  groupHeader: {
    gap: '.75rem',
    paddingBlock: '.75rem',
    paddingInline: '1rem',
    alignItems: 'center',
    backgroundColor: colors.surfaceRaised,
    display: 'flex',
    justifyContent: 'space-between',
  },
  groupTitle: {
    margin: 0,
    gap: '.5rem',
    alignItems: 'center',
    display: 'flex',
    fontSize: typography.sizeSm,
    fontWeight: typography.weightSemibold,
  },
  groupCount: {
    color: colors.textSecondary,
    fontSize: '0.75rem',
    fontVariantNumeric: 'tabular-nums',
  },
  materialList: {
    margin: 0,
    padding: 0,
    listStyle: 'none',
  },
  materialRow: {
    gap: '.75rem',
    paddingBlock: '.75rem',
    paddingInline: '1rem',
    alignItems: 'center',
    display: 'flex',
    flexWrap: 'wrap',
    borderTopColor: colors.border,
    borderTopStyle: 'solid',
    borderTopWidth: 1,
    minWidth: 0,
  },
  materialRowFirst: {
    borderTopWidth: 0,
  },
  materialIcon: {
    borderRadius: layout.radius,
    alignItems: 'center',
    backgroundColor: colors.surfaceRaised,
    color: colors.textSecondary,
    display: 'grid',
    flexBasis: 'auto',
    flexGrow: 0,
    flexShrink: 0,
    justifyContent: 'center',
    height: '2.25rem',
    width: '2.25rem',
  },
  materialBody: {
    flexBasis: '0%',
    flexGrow: 1,
    flexShrink: 1,
    minWidth: '12rem',
  },
  materialTitle: {
    margin: 0,
    overflow: 'hidden',
    fontFamily: 'Roboto Mono, monospace',
    fontSize: typography.sizeSm,
    fontWeight: 500,
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
  },
  materialDetail: {
    margin: 0,
    color: colors.textSecondary,
    fontSize: '0.75rem',
    marginTop: '.125rem',
  },
  materialSelect: {
    borderColor: colors.border,
    borderRadius: layout.radius,
    borderStyle: 'solid',
    borderWidth: 1,
    paddingInline: '.5rem',
    backgroundColor: colors.surface,
    color: colors.textPrimary,
    fontSize: '0.75rem',
    height: '2.25rem',
    maxWidth: '12rem',
  },
  statusIndicator: {
    gap: '.375rem',
    alignItems: 'center',
    color: colors.textSecondary,
    display: 'inline-flex',
    fontSize: '0.75rem',
  },
  coursePanelEmpty: {
    marginBlock: '1.5rem',
    marginInline: 0,
    color: colors.textSecondary,
    textAlign: 'center',
  },
});

interface MaterialsPayload {
  materials: ExternalMaterial[];
}
interface UploadPayload {
  material: ExternalMaterial;
}

function MaterialAction({ material }: { material: ExternalMaterial }) {
  if (material.open_url) {
    return (
      <Button size="sm" href={material.open_url} icon={<BookOpenCheck aria-hidden="true" />}>
        Open & mark up
      </Button>
    );
  }
  if (material.download_url) {
    return (
      <Button
        size="sm"
        variant="outline"
        href={material.download_url}
        target="_blank"
        rel="noopener noreferrer"
        icon={<Download aria-hidden="true" />}
      >
        Open file
      </Button>
    );
  }
  return (
    <span {...stylex.props(styles.statusIndicator)}>
      <FileClock /> {statusLabel(material.status)}
    </span>
  );
}

function MaterialTypeSelect({
  material,
  onChanged,
}: {
  material: ExternalMaterial;
  onChanged: (material: ExternalMaterial) => void;
}) {
  const [busy, setBusy] = React.useState(false);
  const [error, setError] = React.useState('');
  const changeType = async (nextType: ExternalMaterialType) => {
    setBusy(true);
    setError('');
    try {
      const payload = await fetchJson<UploadPayload>(
        `/api/v1/courses/${material.course_id}/materials/${material.document_id}`,
        {
          method: 'PATCH',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ material_type: nextType }),
        },
      );
      onChanged(payload.material);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : 'Could not change the type.');
    } finally {
      setBusy(false);
    }
  };
  return (
    <select
      value={material.material_type}
      disabled={busy}
      onChange={(event) => void changeType(materialTypeValue(event.target.value))}
      aria-label={`Document type for ${material.display_name}`}
      title={error || 'Change document type'}
      {...stylex.props(styles.materialSelect)}
    >
      {MATERIAL_TYPES.map((materialType) => (
        <option key={materialType} value={materialType}>
          {TYPE_META[materialType].label}
        </option>
      ))}
    </select>
  );
}

function MaterialRow({
  material,
  onChanged,
  isFirst,
}: {
  material: ExternalMaterial;
  onChanged: (material: ExternalMaterial) => void;
  isFirst: boolean;
}) {
  const detail = [
    material.document_kind,
    material.page_count ? `${material.page_count} pages` : '',
    formatFileSize(material.source_size_bytes),
    material.source_label || '',
    classificationLabel(material.classification_source),
    material.source_modified_at ? formatTimestamp(material.source_modified_at) : '',
  ].filter(Boolean);
  return (
    <li {...stylex.props(styles.materialRow, isFirst && styles.materialRowFirst)}>
      <span {...stylex.props(styles.materialIcon)}>
        <FileText aria-hidden="true" />
      </span>
      <div {...stylex.props(styles.materialBody)}>
        <p {...stylex.props(styles.materialTitle)} title={material.display_name}>
          {material.display_name}
        </p>
        <p {...stylex.props(styles.materialDetail)}>{detail.join(' · ') || statusLabel(material.status)}</p>
      </div>
      <MaterialTypeSelect material={material} onChanged={onChanged} />
      <MaterialAction material={material} />
    </li>
  );
}

function MaterialGroup({
  materialType,
  materials,
  onChanged,
}: {
  materialType: ExternalMaterialType;
  materials: ExternalMaterial[];
  onChanged: (material: ExternalMaterial) => void;
}) {
  const meta = TYPE_META[materialType];
  const Icon = meta.icon;
  return (
    <section {...stylex.props(styles.groupSection)}>
      <header {...stylex.props(styles.groupHeader)}>
        <h3 {...stylex.props(styles.groupTitle)}>
          <Icon /> {meta.label}
        </h3>
        <span {...stylex.props(styles.groupCount)}>{materials.length}</span>
      </header>
      <ul {...stylex.props(styles.materialList)}>
        {materials.map((material, idx) => (
          <MaterialRow key={material.document_id} material={material} onChanged={onChanged} isFirst={idx === 0} />
        ))}
      </ul>
    </section>
  );
}

function useMaterialPolling(
  courseId: string | number,
  setMaterials: React.Dispatch<React.SetStateAction<ExternalMaterial[]>>,
) {
  const [trackedId, setTrackedId] = React.useState<string | null>(null);
  React.useEffect(() => setTrackedId(null), [courseId]);
  React.useEffect(() => {
    if (!trackedId) return undefined;
    let cancelled = false;
    let attempts = 0;
    const poll = async () => {
      attempts += 1;
      try {
        const payload = await fetchJson<MaterialsPayload>(`/api/v1/courses/${courseId}/materials`);
        if (cancelled) return;
        const found = payload.materials.find((item) => item.document_id === trackedId);
        setMaterials((current) =>
          found || attempts >= 20
            ? payload.materials
            : [...payload.materials, ...current.filter((item) => item.document_id === trackedId)],
        );
        if (found && !['pending', 'running'].includes(found.status) && found.classification_source !== 'pending')
          return setTrackedId(null);
      } catch {
        /* Keep the optimistic row and try again within the bounded window. */
      }
      if (!cancelled && attempts < 20) window.setTimeout(poll, 3000);
    };
    const timer = window.setTimeout(poll, 1500);
    return () => {
      cancelled = true;
      window.clearTimeout(timer);
    };
  }, [courseId, setMaterials, trackedId]);
  return setTrackedId;
}

export default function ExternalMaterials({
  courseId,
  initialMaterials,
  query,
}: {
  courseId: string | number;
  initialMaterials: ExternalMaterial[];
  query: string;
}) {
  const [materials, setMaterials] = React.useState(initialMaterials);
  React.useEffect(() => setMaterials(initialMaterials), [courseId, initialMaterials]);
  const track = useMaterialPolling(courseId, setMaterials);
  const visible = materials.filter((material) => materialMatches(material, query));
  const groups = MATERIAL_TYPES.map((materialType) => ({
    materialType,
    materials: visible.filter((item) => item.material_type === materialType),
  })).filter((group) => group.materials.length);
  const replaceMaterial = (material: ExternalMaterial) => {
    setMaterials((current) => [material, ...current.filter((item) => item.document_id !== material.document_id)]);
    track(material.document_id);
  };
  return (
    <div {...stylex.props(styles.externalMaterials)}>
      <header {...stylex.props(styles.externalHeader)}>
        <BookOpenText {...stylex.props(styles.headerIcon)} aria-hidden="true" />
        <div>
          <h2 {...stylex.props(styles.headerTitle)}>External library</h2>
          <p {...stylex.props(styles.headerDesc)}>
            Collected material, indexed separately from the managed eClass mirror.
          </p>
        </div>
      </header>
      <MaterialUploadPanel courseId={courseId} onUploaded={replaceMaterial} />
      {groups.map((group) => (
        <MaterialGroup key={group.materialType} {...group} onChanged={replaceMaterial} />
      ))}
      {!groups.length && materials.length > 0 ? (
        <p {...stylex.props(styles.coursePanelEmpty)}>No matching learner-added files.</p>
      ) : null}
      {!materials.length ? (
        <p {...stylex.props(styles.coursePanelEmpty)}>
          No files in this course’s external folder yet. Add a file with the uploader above.
        </p>
      ) : null}
    </div>
  );
}
