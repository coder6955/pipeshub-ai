/** The project Files card shows a split file's roll-up in place of its stored COMPLETED. */
import React from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { act, cleanup, render, screen } from '@testing-library/react';
import { Theme } from '@radix-ui/themes';
import '@/lib/__tests__/test-i18n';

const hubApi = vi.hoisted(() => ({ getNodeChildren: vi.fn() }));
vi.mock('../../hooks/use-kb-stream-upload', () => ({ useKbStreamUpload: () => ({ uploadFiles: vi.fn() }) }));
vi.mock('@/chat/project-api', () => ({ ProjectApi: {} }));
vi.mock('@/knowledge-base/api', () => ({ KnowledgeHubApi: hubApi, KnowledgeBaseApi: {} }));
vi.mock('@/app/components/ui/MaterialIcon', () => ({ MaterialIcon: ({ name }: { name: string }) => <i data-icon={name} /> }));

import { FilesCard } from '../files-card';

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});

const file = (id: string, fields: Record<string, unknown>) => ({
  id, name: `${id}.pdf`, nodeType: 'record', indexingStatus: 'COMPLETED', ...fields,
});

async function renderCard(items: unknown[]) {
  hubApi.getNodeChildren.mockResolvedValue({ items });
  render(
    <Theme>
      <FilesCard projectId="p1" linkedKnowledgeBaseId="kb-1" canEdit={false} onKbCreated={vi.fn()} />
    </Theme>,
  );
  await act(async () => {});
}

describe('the project Files card status mark', () => {
  it('names the roll-up of a split file, and the stored status of any other', async () => {
    await renderCard([
      file('running', { splitState: 'INDEXING_PARTS', splitDone: 1, splitTotal: 4 }),
      file('partial', { splitState: 'PARTIAL', splitDone: 4, splitTotal: 4, splitFailed: 1 }),
      file('plain', {}),
    ]);

    const labels = screen.getAllByRole('img').map((el) => el.getAttribute('aria-label'));

    expect(labels[0]).toContain('Indexing 1 of 4 parts');
    expect(labels[1]).toContain('1 of 4 parts failed');
    expect(labels[2]).toContain('Completed');
    expect(labels.slice(0, 2).join()).not.toContain('Completed');
  });
});
