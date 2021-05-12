/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a commercial license.
 * You may not use this file except in compliance with the commercial license.
 */

import {
  observable,
  makeObservable,
  action,
  computed,
  when,
  IReactionDisposer,
  override,
} from 'mobx';
import {currentInstanceStore} from 'modules/stores/currentInstance';
import {fetchFlowNodeInstances} from 'modules/api/flowNodeInstances';
import {logger} from 'modules/logger';
import {NetworkReconnectionHandler} from './networkReconnectionHandler';

const PAGE_SIZE = 50;

type FlowNodeInstanceType = {
  id: string;
  type: string;
  state?: InstanceEntityState;
  flowNodeId: string;
  startDate: string;
  endDate: null | string;
  treePath: string;
  sortValues: any[];
};

type FlowNodeInstances = {
  [treePath: string]: {
    running: boolean | null;
    children: FlowNodeInstanceType[];
  };
};

type State = {
  status:
    | 'initial'
    | 'first-fetch'
    | 'fetching'
    | 'fetched'
    | 'error'
    | 'fetching-next';
  flowNodeInstances: FlowNodeInstances;
};

const DEFAULT_STATE: State = {
  status: 'initial',
  flowNodeInstances: {},
};

class FlowNodeInstance extends NetworkReconnectionHandler {
  state: State = {...DEFAULT_STATE};
  intervalId: null | ReturnType<typeof setInterval> = null;
  disposer: null | IReactionDisposer = null;
  instanceExecutionHistoryDisposer: null | IReactionDisposer = null;
  instanceFinishedDisposer: null | IReactionDisposer = null;

  constructor() {
    super();
    makeObservable(this, {
      state: observable,
      handleFetchSuccess: action,
      handleFetchFailure: action,
      handlePollSuccess: action,
      removeSubTree: action,
      startFetch: action,
      startFetchNext: action,
      reset: override,
      isInstanceExecutionHistoryAvailable: computed,
      instanceExecutionHistory: computed,
    });
  }

  init() {
    this.instanceExecutionHistoryDisposer = when(
      () => currentInstanceStore.state.instance?.id !== undefined,
      () => {
        const instanceId = currentInstanceStore.state.instance?.id;
        if (instanceId !== undefined) {
          this.fetchInstanceExecutionHistory(instanceId);
          this.startPolling();
        }
      }
    );

    this.instanceFinishedDisposer = when(
      () => currentInstanceStore.isRunning === false,
      () => this.stopPolling()
    );
  }

  pollInstances = async () => {
    const processInstanceId = currentInstanceStore.state.instance?.id;
    const {isRunning} = currentInstanceStore;
    if (processInstanceId === undefined || !isRunning) {
      return;
    }

    const queries = Object.entries(this.state.flowNodeInstances)
      .filter(([treePath, flowNodeInstance]) => {
        return flowNodeInstance.running || treePath === processInstanceId;
      })
      .map(([treePath, flowNodeInstance]) => {
        if (treePath === processInstanceId) {
          return {
            treePath,
            processInstanceId,
            pageSize:
              // round up to a multiple of a page size
              Math.ceil(flowNodeInstance.children.length / PAGE_SIZE) *
              PAGE_SIZE,
            searchAfterOrEqual: flowNodeInstance.children[0].sortValues,
          };
        }

        return {treePath, processInstanceId};
      });

    if (queries.length === 0) {
      return;
    }

    try {
      const response = await fetchFlowNodeInstances(queries);

      if (response.ok) {
        if (this.intervalId !== null) {
          this.handlePollSuccess(await response.json());
        }
      } else {
        this.handleFetchFailure();
      }
    } catch (error) {
      this.handleFetchFailure(error);
    }
  };

  fetchNext = async (treePath: string) => {
    const processInstanceId = currentInstanceStore.state.instance?.id;
    if (processInstanceId === undefined) {
      return;
    }

    if (['fetching-next', 'fetching'].includes(this.state.status)) {
      return;
    }

    const children = this.state.flowNodeInstances[treePath].children;
    this.stopPolling();
    this.startFetchNext();
    try {
      const response = await fetchFlowNodeInstances([
        {
          processInstanceId,
          treePath,
          pageSize: PAGE_SIZE,
          searchAfter: children[children.length - 1].sortValues,
        },
      ]);

      if (response.ok) {
        const flowNodeInstances: FlowNodeInstances = await response.json();

        flowNodeInstances[treePath].children = [
          ...this.state.flowNodeInstances[treePath].children,
          ...flowNodeInstances[treePath].children,
        ];

        this.handleFetchSuccess(flowNodeInstances);
        this.startPolling();
      } else {
        this.handleFetchFailure();
      }
    } catch (error) {
      this.handleFetchFailure(error);
    }
  };

  fetchSubTree = async ({
    treePath,
    pageSize,
    searchAfter,
  }: {
    treePath: string;
    pageSize?: number;
    searchAfter?: FlowNodeInstanceType['sortValues'];
  }) => {
    const processInstanceId = currentInstanceStore.state.instance?.id;

    if (processInstanceId === undefined) {
      return;
    }

    try {
      const response = await fetchFlowNodeInstances([
        {
          processInstanceId: processInstanceId,
          treePath,
          pageSize,
          searchAfter,
        },
      ]);

      if (response.ok) {
        this.handleFetchSuccess(await response.json());
      } else {
        this.handleFetchFailure();
      }
    } catch (error) {
      this.handleFetchFailure(error);
    }
  };

  removeSubTree = ({treePath}: {treePath: string}) => {
    // remove all nested sub trees first
    Object.keys(this.state.flowNodeInstances)
      .filter((currentTreePath) => {
        return currentTreePath.match(new RegExp(`^${treePath}/`));
      })
      .forEach((currentTreePath) => {
        delete this.state.flowNodeInstances[currentTreePath];
      });

    delete this.state.flowNodeInstances[treePath];
  };

  fetchInstanceExecutionHistory = this.retryOnConnectionLost(
    async (processInstanceId: ProcessInstanceEntity['id']) => {
      this.startFetch();
      this.fetchSubTree({
        treePath: processInstanceId,
        pageSize: PAGE_SIZE,
      });
    }
  );

  startFetch = () => {
    if (this.state.status === 'initial') {
      this.state.status = 'first-fetch';
    } else {
      this.state.status = 'fetching';
    }
  };

  startFetchNext = () => {
    this.state.status = 'fetching-next';
  };

  handleFetchFailure = (error?: Error) => {
    this.state.status = 'error';
    logger.error('Failed to fetch Instances activity');
    if (error !== undefined) {
      logger.error(error);
    }
  };

  handleFetchSuccess = (flowNodeInstances: FlowNodeInstances) => {
    Object.entries(flowNodeInstances).forEach(
      ([treePath, flowNodeInstance]) => {
        this.state.flowNodeInstances[treePath] = flowNodeInstance;
      }
    );

    this.state.status = 'fetched';
  };

  handlePollSuccess = (flowNodeInstances: FlowNodeInstances) => {
    if (this.intervalId === null) {
      return;
    }

    Object.entries(flowNodeInstances).forEach(
      ([treePath, flowNodeInstance]) => {
        // don't create new trees (this prevents showing a tree when the user collapsed it earlier)
        if (this.state.flowNodeInstances[treePath] !== undefined) {
          this.state.flowNodeInstances[treePath] = flowNodeInstance;
        }
      }
    );
  };

  startPolling = () => {
    if (currentInstanceStore.isRunning && this.intervalId === null) {
      this.intervalId = setInterval(this.pollInstances, 5000);
    }
  };

  stopPolling = () => {
    const {intervalId} = this;

    if (intervalId !== null) {
      clearInterval(intervalId);
      this.intervalId = null;
    }
  };

  reset() {
    super.reset();
    this.stopPolling();
    this.state = {...DEFAULT_STATE};
    this.disposer?.();
    this.instanceExecutionHistoryDisposer?.();
    this.instanceFinishedDisposer?.();
  }

  get isInstanceExecutionHistoryAvailable() {
    const {status} = this.state;

    return (
      ['fetched', 'fetching-next', 'fetching-prev'].includes(status) &&
      this.instanceExecutionHistory !== null &&
      Object.keys(this.instanceExecutionHistory).length > 0
    );
  }

  get instanceExecutionHistory(): FlowNodeInstanceType | null {
    const {instance: processInstance} = currentInstanceStore.state;
    const {status} = this.state;

    if (
      processInstance === null ||
      ['initial', 'first-fetch'].includes(status)
    ) {
      return null;
    }

    return {
      id: processInstance.id,
      type: 'PROCESS',
      state: processInstance.state,
      treePath: processInstance.id,
      endDate: null,
      startDate: '',
      sortValues: [],
      flowNodeId: processInstance.processId,
    };
  }
}

export const flowNodeInstanceStore = new FlowNodeInstance();
export type {FlowNodeInstanceType as FlowNodeInstance, FlowNodeInstances};
