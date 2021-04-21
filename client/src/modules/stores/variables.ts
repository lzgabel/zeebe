/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a commercial license.
 * You may not use this file except in compliance with the commercial license.
 */

import {
  makeObservable,
  observable,
  action,
  computed,
  when,
  autorun,
  IReactionDisposer,
  override,
} from 'mobx';
import {fetchVariables, applyOperation} from 'modules/api/instances';
import {differenceWith, differenceBy} from 'lodash';
import {currentInstanceStore} from 'modules/stores/currentInstance';
import {flowNodeSelectionStore} from 'modules/stores/flowNodeSelection';
import {STATE} from 'modules/constants';
import {isInstanceRunning} from './utils/isInstanceRunning';
import {logger} from 'modules/logger';
import {flowNodeMetaDataStore} from './flowNodeMetaData';
import {NetworkReconnectionHandler} from './networkReconnectionHandler';

type State = {
  items: VariableEntity[];
  status: 'initial' | 'first-fetch' | 'fetching' | 'fetched' | 'error';
};

const DEFAULT_STATE: State = {
  items: [],
  status: 'initial',
};

class Variables extends NetworkReconnectionHandler {
  state: State = {
    ...DEFAULT_STATE,
  };
  shouldCancelOngoingRequests: boolean = false;
  intervalId: null | ReturnType<typeof setInterval> = null;
  disposer: null | IReactionDisposer = null;
  variablesWithActiveOperationsDisposer: null | IReactionDisposer = null;
  fetchVariablesDisposer: null | IReactionDisposer = null;

  constructor() {
    super();
    makeObservable(this, {
      state: observable,
      reset: override,
      setItems: action,
      handleFetchSuccess: action,
      startFetch: action,
      handleFetchFailure: action,
      clearItems: action,
      updateVariable: action,
      addVariable: action,
      setSingleVariable: action,
      hasNoVariables: computed,
      hasActiveOperation: computed,
      scopeId: computed,
      displayStatus: computed,
    });
  }

  init = (instanceId: ProcessInstanceEntity['id']) => {
    this.variablesWithActiveOperationsDisposer = when(
      () => currentInstanceStore.state.instance?.state === STATE.CANCELED,
      this.removeVariablesWithActiveOperations
    );

    this.disposer = autorun(() => {
      if (
        isInstanceRunning(currentInstanceStore.state.instance) &&
        this.scopeId !== null
      ) {
        if (this.intervalId === null) {
          this.startPolling(instanceId);
        }
      } else {
        this.stopPolling();
      }
    });

    this.fetchVariablesDisposer = autorun(() => {
      if (this.scopeId !== null) {
        this.clearItems();
        this.fetchVariables(instanceId);
      }
    });
  };

  clearItems = () => {
    this.state.items = [];
  };

  handleFetchFailure = (error?: Error) => {
    this.state.status = 'error';
    logger.error('Failed to fetch Variables');
    if (error !== undefined) {
      logger.error(error);
    }
  };

  handleFetchSuccess = () => {
    this.state.status = 'fetched';
  };

  startFetch = () => {
    if (this.state.status === 'initial') {
      this.state.status = 'first-fetch';
    } else {
      this.state.status = 'fetching';
    }
  };

  setItems = (items: VariableEntity[]) => {
    this.state.items = items;
  };

  get scopeId() {
    const {selection} = flowNodeSelectionStore.state;
    const {metaData} = flowNodeMetaDataStore.state;

    return (
      (selection && selection.flowNodeInstanceId) ||
      (metaData && metaData.flowNodeInstanceId)
    );
  }

  handlePolling = async (processInstanceId: ProcessInstanceEntity['id']) => {
    try {
      const response = await fetchVariables({
        instanceId: processInstanceId,
        scopeId: this.scopeId || processInstanceId,
      });

      if (this.shouldCancelOngoingRequests) {
        this.shouldCancelOngoingRequests = false;
        return;
      }

      if (this.intervalId !== null && response.ok) {
        this.handleResponse(await response.json());
      }

      if (!response.ok) {
        logger.error('Failed to poll Variables');
      }
    } catch (error) {
      logger.error('Failed to poll Variables');
      logger.error(error);
    }
  };

  handleResponse = async (response: VariableEntity[]) => {
    if (this.state.items.length === 0) {
      this.setItems(response);
    } else {
      const {items} = this.state;
      const localVariables = differenceWith(
        items,
        response,
        (item, {name, value}) => item.name === name && item.value === value
      );
      const serverVariables = differenceBy(response, localVariables, 'name');

      this.setItems([...serverVariables, ...localVariables]);
    }

    this.handleFetchSuccess();
  };

  fetchVariables = this.retryOnConnectionLost(
    async (instanceId: ProcessInstanceEntity['id']) => {
      this.startFetch();

      try {
        const response = await fetchVariables({
          instanceId,
          scopeId: this.scopeId ?? instanceId,
        });

        if (this.shouldCancelOngoingRequests) {
          this.shouldCancelOngoingRequests = false;
          return;
        }

        if (response.ok) {
          this.handleResponse(await response.json());
          this.handleFetchSuccess();
        } else {
          this.handleFetchFailure();
        }
      } catch (error) {
        this.handleFetchFailure(error);
      }
    }
  );

  setSingleVariable = (variable: VariableEntity) => {
    const {items} = this.state;
    this.state.items = items.map((item) => {
      if (item.name === variable.name) {
        return variable;
      }

      return item;
    });
  };

  addVariable = async ({
    id,
    name,
    value,
    onError,
  }: {
    id: string;
    name: string;
    value: string;
    onError: () => void;
  }) => {
    this.setItems([
      ...this.state.items,
      {
        name,
        value,
        hasActiveOperation: true,
        processInstanceId: id,
      },
    ]);

    try {
      const response = await applyOperation(id, {
        operationType: 'UPDATE_VARIABLE',
        variableScopeId: this.scopeId || undefined,
        variableName: name,
        variableValue: value,
      });

      if (!response.ok) {
        this.setItems(this.state.items.filter((item) => item.name !== name));
        onError();
      }
    } catch {
      this.setItems(this.state.items.filter((item) => item.name !== name));
      onError();
    }
  };

  updateVariable = async ({
    id,
    name,
    value,
    onError,
  }: {
    id: string;
    name: string;
    value: string;
    onError: () => void;
  }) => {
    const {items} = this.state;

    const originalVariable = items.find((item) => item.name === name);
    if (originalVariable === undefined) {
      return;
    }

    this.setSingleVariable({
      ...originalVariable,
      value,
      hasActiveOperation: true,
    });

    try {
      const response = await applyOperation(id, {
        operationType: 'UPDATE_VARIABLE',
        variableScopeId: this.scopeId || undefined,
        variableName: name,
        variableValue: value,
      });

      if (!response.ok) {
        this.setSingleVariable(originalVariable);
        onError();
      }
    } catch {
      this.setSingleVariable(originalVariable);
      onError();
    }
  };

  get hasActiveOperation() {
    const {items} = this.state;
    return items.some(({hasActiveOperation}) => hasActiveOperation);
  }

  get hasNoVariables() {
    const {status, items} = this.state;
    return status === 'fetched' && items.length === 0;
  }

  removeVariablesWithActiveOperations = () => {
    this.state.items = this.state.items.filter(
      ({hasActiveOperation}) => !hasActiveOperation
    );
  };

  startPolling = async (instanceId: string) => {
    this.intervalId = setInterval(() => {
      this.handlePolling(instanceId);
    }, 5000);
  };

  stopPolling = () => {
    const {intervalId} = this;

    if (intervalId !== null) {
      clearInterval(intervalId);
      this.intervalId = null;
    }
  };

  get displayStatus() {
    const {status, items} = this.state;

    if (status === 'error') {
      return 'error';
    }
    if (['initial', 'first-fetch'].includes(status)) {
      return 'skeleton';
    }
    if (flowNodeMetaDataStore.hasMultipleInstances) {
      return 'multi-instances';
    }
    if (status === 'fetching' || this.scopeId === null) {
      return 'spinner';
    }
    if (this.hasNoVariables) {
      return 'no-variables';
    }
    if (status === 'fetched' && items.length > 0) {
      return 'variables';
    }

    logger.error('Failed to show Variables');
    return 'error';
  }

  reset() {
    super.reset();
    if (['first-fetch', 'fetching'].includes(this.state.status)) {
      this.shouldCancelOngoingRequests = true;
    }
    this.stopPolling();
    this.state = {...DEFAULT_STATE};
    this.disposer?.();
    this.variablesWithActiveOperationsDisposer?.();
    this.fetchVariablesDisposer?.();
  }
}

export const variablesStore = new Variables();
