import { useMemo } from 'react';
import type { ExternalMaterial, ExternalMaterialType } from '@/lib/types';
import { AppLink } from '../app/links';
import { Select } from '../ui/forms';
import { Icon, Text, View } from '../ui/primitives';
import { materialDetail, materialId, materialTypeValue, statusLabel, TYPE_LABELS, TYPE_OPTIONS } from './helpers';
import type { MaterialGroup } from './helpers';
import type { MaterialEdits } from './mutations';
import { styles } from './styles';

interface MaterialGroupsProps {
  courseId: number | string;
  groups: MaterialGroup[];
  edits: MaterialEdits;
  onTypeChange: (material: ExternalMaterial, nextType: ExternalMaterialType) => Promise<void>;
}

function MaterialAction({ material }: { material: ExternalMaterial }) {
  const id = materialId(material);
  if (material.open_url) {
    return (
      <AppLink id={`${id}:open`} href={material.open_url} target="_self" style={styles.link}>
        Open &amp; mark up
      </AppLink>
    );
  }
  if (material.download_url) {
    return (
      <AppLink
        id={`${id}:download`}
        href={material.download_url}
        target="_blank"
        rel="noopener noreferrer"
        style={styles.link}
      >
        Open file
      </AppLink>
    );
  }
  return (
    <Text id={`${id}:status`} style={styles.status}>
      {statusLabel(material.status)}
    </Text>
  );
}

function MaterialRow({
  material,
  edits,
  onTypeChange,
}: {
  material: ExternalMaterial;
  edits: MaterialEdits;
  onTypeChange: MaterialGroupsProps['onTypeChange'];
}) {
  const id = materialId(material);
  const detail = useMemo(() => materialDetail(material), [material]);
  const edit = edits[material.document_id];
  return (
    <View id={id} style={styles.row}>
      <Icon name="file" style={styles.rowIcon} />
      <View style={styles.rowBody}>
        <Text id={`${id}:title`} title={material.display_name} style={styles.rowTitle}>
          {material.display_name}
        </Text>
        <Text id={`${id}:detail`} style={styles.muted}>
          {detail}
        </Text>
      </View>
      <View style={styles.rowControls}>
        <Select
          id={`${id}:type`}
          label={`Document type for ${material.display_name}`}
          title={edit?.error || 'Change document type'}
          value={material.material_type}
          options={TYPE_OPTIONS}
          disabled={edit?.busy}
          onChange={(value) => {
            void onTypeChange(material, materialTypeValue(value));
          }}
        />
        <MaterialAction material={material} />
      </View>
    </View>
  );
}

export function MaterialGroups({ courseId, groups, edits, onTypeChange }: MaterialGroupsProps) {
  return (
    <View style={styles.groups}>
      {groups.map(({ type, materials }) => (
        <View key={type} id={`materials:${encodeURIComponent(String(courseId))}:group:${type}`} style={styles.group}>
          <View style={styles.groupHeader}>
            <Text style={styles.groupTitle}>{TYPE_LABELS[type]}</Text>
            <Text style={styles.muted}>{materials.length}</Text>
          </View>
          {materials.map((material) => (
            <MaterialRow key={material.document_id} material={material} edits={edits} onTypeChange={onTypeChange} />
          ))}
        </View>
      ))}
    </View>
  );
}
