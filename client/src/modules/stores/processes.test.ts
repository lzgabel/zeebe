/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a commercial license.
 * You may not use this file except in compliance with the commercial license.
 */

import {processesStore} from './processes';
import {rest} from 'msw';
import {mockServer} from 'modules/mock-server/node';
import {waitFor} from '@testing-library/react';
import {groupedProcessesMock} from 'modules/testUtils';

describe('stores/processes', () => {
  it('should retry fetch on network reconnection', async () => {
    const eventListeners: any = {};
    const originalEventListener = window.addEventListener;
    window.addEventListener = jest.fn((event: string, cb: any) => {
      eventListeners[event] = cb;
    });

    mockServer.use(
      rest.get('/api/processes/grouped', (_, res, ctx) =>
        res.once(ctx.json(groupedProcessesMock))
      )
    );

    processesStore.fetchProcesses();

    await waitFor(() =>
      expect(processesStore.state.processes).toEqual(groupedProcessesMock)
    );

    const newGroupedProcessesResponse = [groupedProcessesMock[0]];

    mockServer.use(
      rest.get('/api/processes/grouped', (_, res, ctx) =>
        res.once(ctx.json(newGroupedProcessesResponse))
      )
    );

    eventListeners.online();

    await waitFor(() =>
      expect(processesStore.state.processes).toEqual(
        newGroupedProcessesResponse
      )
    );

    window.addEventListener = originalEventListener;
  });
});
