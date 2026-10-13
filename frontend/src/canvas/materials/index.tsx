import { useMemo, useState } from 'react';
import type { ExternalMaterial } from '@/lib/types';
import { Button, Notice } from '../ui/controls';
import { Text, View } from '../ui/primitives';
import { groupMaterials } from './helpers';
import { useInitialMaterials } from './load';
import { MaterialGroups } from './rows';
import { useMaterialLibrary } from './state';
import { styles } from './styles';
import { MaterialUploadPanel } from './upload';

export interface ExternalMaterialsProps {
  courseId: number | string;
  search: string;
  onPublished?: () => void;
}

function MaterialsLibrary({
  courseId,
  search,
  onPublished,
  initialMaterials,
}: ExternalMaterialsProps & {
  initialMaterials: ExternalMaterial[];
}) {
  const state = useMaterialLibrary(courseId, initialMaterials, onPublished);
  const groups = useMemo(() => groupMaterials(state.materials, search), [state.materials, search]);
  return (
    <View style={styles.library}>
      <View style={styles.header}>
        <Text style={styles.title}>External library</Text>
        <Text style={styles.muted}>Collected material, indexed separately from the managed eClass mirror.</Text>
      </View>
      <MaterialUploadPanel courseId={courseId} onUploaded={state.uploaded} />
      <MaterialGroups courseId={courseId} groups={groups} edits={state.edits} onTypeChange={state.changeType} />
      {!groups.length && state.materials.length > 0 && (
        <Text style={styles.empty}>No matching learner-added files.</Text>
      )}
      {!state.materials.length && (
        <Text style={styles.empty}>
          No files in this course’s external folder yet. Add a file with the uploader above.
        </Text>
      )}
    </View>
  );
}

function ExternalLibraryDisclosure(props: ExternalMaterialsProps) {
  const [open, setOpen] = useState(false);
  const state = useInitialMaterials(props.courseId, open);
  const id = `materials:${encodeURIComponent(String(props.courseId))}`;
  return (
    <View id={id}>
      <Button
        id={`${id}:disclosure`}
        expanded={open}
        controls={`${id}:content`}
        onClick={() => setOpen((value) => !value)}
        style={styles.disclosure}
      >
        <Text>External library</Text>
      </Button>
      {open && state.failed && (
        <Notice id={`${id}:error`} tone="danger">
          <View style={styles.failure}>
            <Text>External library could not load.</Text>
            <Button id={`${id}:retry`} onClick={state.retry}>
              <Text>Try again</Text>
            </Button>
          </View>
        </Notice>
      )}
      {open && state.materials === null && !state.failed && (
        <Text id={`${id}:loading`} style={styles.loading}>
          Loading external library…
        </Text>
      )}
      {state.materials !== null && (
        <View id={`${id}:content`} style={open ? undefined : styles.hidden}>
          <MaterialsLibrary {...props} initialMaterials={state.materials} />
        </View>
      )}
    </View>
  );
}

export function ExternalMaterials(props: ExternalMaterialsProps) {
  return <ExternalLibraryDisclosure key={String(props.courseId)} {...props} />;
}
