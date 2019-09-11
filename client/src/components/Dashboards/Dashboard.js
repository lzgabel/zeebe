/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a commercial license.
 * You may not use this file except in compliance with the commercial license.
 */

import React from 'react';
import {Redirect} from 'react-router-dom';
import moment from 'moment';

import {withErrorHandling} from 'HOC';
import {loadEntity, deleteEntity, updateEntity, createEntity} from 'services';

import {ErrorPage, LoadingIndicator} from 'components';

import {addNotification} from 'notifications';
import {t} from 'translation';

import {isAuthorizedToShareDashboard, isSharingEnabled} from './service';

import DashboardView from './DashboardView';
import DashboardEdit from './DashboardEdit';

import './Dashboard.scss';

export default withErrorHandling(
  class Dashboard extends React.Component {
    constructor(props) {
      super(props);

      this.state = {
        name: null,
        lastModified: null,
        lastModifier: null,
        loaded: false,
        redirect: '',
        reports: [],
        serverError: null,
        isAuthorizedToShare: false,
        sharingEnabled: false
      };
    }

    getId = () => this.props.match.params.id;
    isNew = () => this.getId() === 'new';

    componentDidMount = async () => {
      this.setState({sharingEnabled: await isSharingEnabled()});

      if (this.isNew()) {
        this.createDashboard();
      } else {
        this.loadDashboard();
      }
    };

    createDashboard = () => {
      this.setState({
        loaded: true,
        name: t('dashboard.new'),
        lastModified: moment().format('Y-MM-DDTHH:mm:ss.SSSZZ'),
        lastModifier: t('common.you'),
        reports: [],
        isAuthorizedToShare: true
      });
    };

    loadDashboard = () => {
      this.props.mightFail(
        loadEntity('dashboard', this.getId()),
        async response => {
          const {name, lastModifier, lastModified, reports} = response;

          this.setState({
            lastModifier,
            lastModified,
            loaded: true,
            name,
            reports: reports || [],
            isAuthorizedToShare: await isAuthorizedToShareDashboard(this.getId())
          });
        },
        ({status}) => {
          this.setState({
            serverError: status
          });
        }
      );
    };

    deleteDashboard = async evt => {
      await deleteEntity('dashboard', this.getId());

      this.setState({
        redirect: '/'
      });
    };

    updateDashboard = (id, name, reports) => {
      this.props.mightFail(
        updateEntity('dashboard', id, {
          name,
          reports
        }),
        async () => {
          this.setState({
            name,
            reports,
            redirect: this.isNew() ? `/dashboard/${id}/` : './',
            isAuthorizedToShare: await isAuthorizedToShareDashboard(id)
          });
        },
        () => {
          addNotification({text: t('dashboard.cannotSave', {name}), type: 'error'});
        }
      );
    };

    saveChanges = (name, reports) => {
      if (this.isNew()) {
        this.props.mightFail(
          createEntity('dashboard'),
          id => this.updateDashboard(id, name, reports),
          () => {
            addNotification({text: t('dashboard.cannotSave', {name}), type: 'error'});
          }
        );
      } else {
        this.updateDashboard(this.getId(), name, reports);
      }
    };

    componentDidUpdate() {
      if (this.state.redirect) {
        this.setState({redirect: ''});
      }
    }

    render() {
      const {viewMode} = this.props.match.params;

      const {
        loaded,
        redirect,
        serverError,
        name,
        lastModified,
        lastModifier,
        sharingEnabled,
        isAuthorizedToShare,
        reports
      } = this.state;

      if (serverError) {
        return <ErrorPage />;
      }

      if (!loaded) {
        return <LoadingIndicator />;
      }

      if (redirect) {
        return <Redirect to={redirect} />;
      }

      const commonProps = {
        name,
        lastModified,
        lastModifier,
        id: this.getId()
      };

      return (
        <div className="Dashboard">
          {viewMode === 'edit' ? (
            <DashboardEdit
              {...commonProps}
              isNew={this.isNew()}
              saveChanges={this.saveChanges}
              initialReports={this.state.reports}
            />
          ) : (
            <DashboardView
              {...commonProps}
              sharingEnabled={sharingEnabled}
              isAuthorizedToShare={isAuthorizedToShare}
              loadDashboard={this.loadDashboard}
              deleteDashboard={this.deleteDashboard}
              reports={reports}
            />
          )}
        </div>
      );
    }
  }
);
